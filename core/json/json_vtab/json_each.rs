use std::fmt::Display;

use limbo_ext::{
    register_extension, ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive, Value,
};

use crate::{
    json::{
        get_json, get_json_value, json_extract_single,
        json_path::{json_path, JsonPath, PathElement},
        Val,
    },
    OwnedValue,
};

register_extension! {
    vtabs: { JsonEachVTab }
}

macro_rules! try_option {
    ($expr:expr, $err:expr) => {
        match $expr {
            Some(val) => val,
            None => return $err,
        }
    };
}

/// A virtual table that generates a sequence of integers
#[derive(Debug, VTabModuleDerive, Default)]
struct JsonEachVTab;

impl VTabModule for JsonEachVTab {
    type VCursor = JsonEachCursor;
    type Error = ResultCode;
    const NAME: &'static str = "json_each";
    const VTAB_KIND: VTabKind = VTabKind::TableValuedFunction;

    fn create_schema(_args: &[Value]) -> String {
        // Create table schema
        "CREATE TABLE json_each(
            key ANY,             -- key for current element relative to its parent
            value ANY,           -- value for the current element
            type TEXT,           -- 'object','array','string','integer', etc.
            atom ANY,            -- value for primitive types, null for array & object
            id INTEGER,          -- integer ID for this element
            parent INTEGER,      -- integer ID for the parent of this element
            fullkey TEXT,        -- full path describing the current element
            path TEXT,           -- path to the container of the current row
            json JSON HIDDEN,    -- 1st input parameter: the raw JSON
            root TEXT HIDDEN     -- 2nd input parameter: the PATH at which to start
        );"
        .into()
    }

    fn open(&self) -> Result<Self::VCursor, Self::Error> {
        Ok(JsonEachCursor::default())
    }

    fn filter(cursor: &mut Self::VCursor, args: &[Value]) -> ResultCode {
        if args.len() != 1 && args.len() != 2 {
            return ResultCode::InvalidArgs;
        }
        // TODO: For now we are not dealing with JSONB

        let json_val = try_option!(args[0].to_text(), ResultCode::InvalidArgs);

        let json_val = try_option!(
            get_json_value(&OwnedValue::from_text(json_val)).ok(),
            ResultCode::InvalidArgs // Invalid Json
        );
        let path = args[1].to_text().unwrap_or("$");

        let j_path = try_option!(json_path(path).ok(), ResultCode::InvalidArgs);

        cursor.path = j_path;
        cursor.json_val = json_val;

        cursor.next()
    }

    fn column(cursor: &Self::VCursor, idx: u32) -> Result<Value, Self::Error> {
        cursor.column(idx)
    }

    fn next(cursor: &mut Self::VCursor) -> ResultCode {
        cursor.next()
    }

    fn eof(cursor: &Self::VCursor) -> bool {
        cursor.eof()
    }
}

/// The cursor for iterating over the generated sequence
#[derive(Debug)]
struct JsonEachCursor {
    rowid: i64,
    path: JsonPath,
    json_val: Val,  // Initial Val
    key: String,    // Current key
    val: Val,       // Current Json Val
    id: i64,        // Arbitrary id of the value,
    increment: i64, // Value to increment id
    eof: bool,
    ctx: Vec<usize>,
    recursive: bool, // True if we are dealing with json_tree function
    start: bool,     // True if we are starting on to iterate over a new object or array
}

impl Default for JsonEachCursor {
    fn default() -> Self {
        Self {
            rowid: i64::default(),
            path: JsonPath::default(),
            json_val: Val::Null,
            id: -1,
            increment: 1,
            key: "".to_string(),
            val: Val::Null,
            eof: false,
            ctx: Vec::new(),
            recursive: false,
            start: true,
        }
    }
}

impl VTabCursor for JsonEachCursor {
    type Error = ResultCode;

    fn next(&mut self) -> ResultCode {
        if self.eof() {
            return ResultCode::EOF;
        }
        if self.start {
            self.id += 1;
            self.start = false;
        }

        self.rowid += 1;
        self.id += self.increment;

        // TODO Improvement: see a way to first sort the elements so that we can pop from last instead of
        // remove_first and as the Vec shifts every time we remove_first
        match &mut self.json_val {
            Val::Array(v) => {
                if let Some(val) = v.remove_first() {
                    self.key = {
                        if let Some(idx) = self.ctx.last_mut() {
                            *idx += 1;
                            idx.to_string()
                        } else {
                            self.ctx.push(0);
                            0.to_string()
                        }
                    };
                    self.val = val;
                } else {
                    let _ = self.ctx.pop();
                    self.eof = true;
                    return ResultCode::EOF;
                }
            }
            Val::Object(v) => {
                if let Some((key, val)) = v.remove_first() {
                    self.val = val;
                    self.key = key;
                } else {
                    self.eof = true;
                    return ResultCode::EOF;
                }
            }
            Val::Removed => unreachable!(),
            _ => self.eof = true, // This means to return the self.json_val in column
        };

        if self.recursive {
            self.increment = 1;
        } else {
            self.increment = self.val.key_value_count() as i64;
            dbg!(&self.increment);
        }

        ResultCode::OK
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        let ret_val = {
            if self.eof() {
                &self.json_val
            } else {
                &self.val
            }
        };

        let result = match idx {
            0 => Value::from_text(self.key.to_owned()), // Key
            1 => ret_val.to_value(),                    // Value
            2 => Value::from_text(ret_val.type_name()), // Type
            3 => ret_val.atom_value(),                  // Atom
            4 => Value::from_integer(self.id),
            _ => Value::null(),
        };
        Ok(result)
    }

    fn rowid(&self) -> i64 {
        self.rowid
    }
}

impl Val {
    fn type_name(&self) -> String {
        let val = match self {
            Val::Null => "null",
            Val::Bool(v) => {
                if *v {
                    "true"
                } else {
                    "false"
                }
            }
            Val::Integer(_) => "integer",
            Val::Float(_) => "real",
            Val::String(_) => "text",
            Val::Array(_) => "array",
            Val::Object(_) => "object",
            Val::Removed => unreachable!(),
        };
        val.to_string()
    }

    fn to_value(&self) -> Value {
        match self {
            Val::Null => Value::null(),
            Val::Bool(v) => {
                if *v {
                    Value::from_integer(1)
                } else {
                    Value::from_integer(0)
                }
            }
            Val::Integer(v) => Value::from_integer(*v),
            Val::Float(v) => Value::from_float(*v),
            Val::String(v) => Value::from_text(v.clone()),
            Val::Removed => unreachable!(),
            // TODO: as we cannot declare a subtype for JSON I have to return text here
            v => Value::from_text(v.to_string()),
        }
    }

    fn atom_value(&self) -> Value {
        match self {
            Val::Null => Value::null(),
            Val::Bool(v) => {
                if *v {
                    Value::from_integer(1)
                } else {
                    Value::from_integer(0)
                }
            }
            Val::Integer(v) => Value::from_integer(*v),
            Val::Float(v) => Value::from_float(*v),
            Val::String(v) => Value::from_text(v.clone()),
            Val::Removed => unreachable!(),
            _ => Value::null(),
        }
    }

    fn key_value_count(&self) -> usize {
        match self {
            Val::Array(v) => v.iter().map(|val| val.key_value_count()).sum(),
            Val::Object(v) => v.iter().map(|(_, val)| val.key_value_count() + 1).sum(),
            Val::Removed => unreachable!(),
            _ => 1,
        }
    }
}

impl Display for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::Null => write!(f, "{}", ""),
            Val::Bool(v) => {
                if *v {
                    write!(f, "{}", "1")
                } else {
                    write!(f, "{}", "0")
                }
            }
            Val::Integer(v) => write!(f, "{}", v),
            Val::Float(v) => write!(f, "{}", v),
            Val::String(v) => write!(f, "{}", v),
            Val::Array(vals) => {
                let mut vals_iter = vals.iter();
                write!(f, "[")?;
                let mut comma = false;
                while let Some(val) = vals_iter.next() {
                    if comma {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", val.to_string())?; // Call format recursively
                    comma = true;
                }
                write!(f, "]")
            }
            Val::Object(vals) => {
                write!(f, "{{")?;
                let mut comma = false;
                for (key, val) in vals {
                    if comma {
                        write!(f, ",")?;
                    }
                    write!(f, "\"{}\": {}", key, val.to_string())?; // Call format recursively
                    comma = true;
                }
                write!(f, "}}")
            }
            Val::Removed => unreachable!(),
        }
    }
}

trait VecExt<T> {
    fn remove_first(&mut self) -> Option<T>;
}

impl<T> VecExt<T> for Vec<T> {
    fn remove_first(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        Some(self.remove(0))
    }
}

#[cfg(test)]
mod tests {}
