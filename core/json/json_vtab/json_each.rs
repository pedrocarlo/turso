use limbo_ext::{
    register_extension, ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive, Value,
};

use crate::json::json_path::{json_path, JsonPath};

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

enum Columns {
    Key,
    Value,
    Type,
    Atom,
    Id,
    Parent,
    FullKey,
    Path,
}

/// A virtual table that generates a sequence of integers
#[derive(Debug, VTabModuleDerive, Default)]
struct JsonEachVTab;

impl VTabModule<'_> for JsonEachVTab {
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
        dbg!(
            "json_each",
            &args,
            args.len(),
            args.len() != 1,
            args.len() != 2,
            args.len() != 1 && args.len() != 2
        );
        if args.len() != 1 && args.len() != 2 {
            return ResultCode::InvalidArgs;
        }
        // For now we are not dealing with JSONB

        let json_val = try_option!(args[0].to_text(), ResultCode::InvalidArgs).to_string();
        let path = args[1].to_text().unwrap_or("$");

        // let j_path: JsonPath<'_> = try_option!(json_path(&path).ok(), ResultCode::InvalidArgs);

        dbg!("json_each");

        // cursor.path = j_path;
        cursor.root = path.to_string();

        todo!();

        ResultCode::OK
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

    fn delete(&mut self, _rowid: i64) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The cursor for iterating over the generated sequence
#[derive(Debug, Default)]
struct JsonEachCursor {
    rowid: i64,
    root: String,
    path: JsonPath,
}

impl VTabCursor<'_> for JsonEachCursor {
    type Error = ResultCode;

    fn next(&mut self) -> ResultCode {
        self.rowid += 1;
        todo!()
    }

    fn eof(&self) -> bool {
        todo!()
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        todo!()
    }

    fn rowid(&self) -> i64 {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
