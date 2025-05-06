pub mod binary_operator;

use std::fmt::Display;

use binary_operator::BinaryOperator;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

use crate::model::table::{Table, Value};

pub trait TestPredicate {
    fn test(&self, row: &[Value], table: &Table) -> bool;

    fn reduce_to_value(&self, row: &[Value], table: &Table) -> Value;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Predicate {
    Literal(Value),
    Column(String),
    BinaryOperator(Box<BinaryOperator>),
    Like(String, String), // column LIKE Value
}

/// This function is a duplication of the exec_like function in core/vdbe/mod.rs at commit 9b9d5f9b4c9920e066ef1237c80878f4c3968524
/// Any updates to the original function should be reflected here, otherwise the test will be incorrect.
fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    for c in pattern.chars() {
        match c {
            '\\' => regex_pattern.push_str("\\\\"),
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            ch => {
                if regex_syntax::is_meta_character(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(ch);
            }
        }
    }

    regex_pattern.push('$');

    RegexBuilder::new(&regex_pattern)
        .case_insensitive(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap()
}

fn exec_like(pattern: &str, text: &str) -> bool {
    let re = construct_like_regex(pattern);
    re.is_match(text)
}

impl Predicate {
    pub(crate) fn true_() -> Self {
        Self::Literal(Value::TRUE)
    }

    pub(crate) fn false_() -> Self {
        Self::Literal(Value::FALSE)
    }
}

impl TestPredicate for Predicate {
    fn test(&self, row: &[Value], table: &Table) -> bool {
        match self {
            Predicate::BinaryOperator(op) => op.test(row, table),
            _ => self.reduce_to_value(row, table).into(),
        }
    }

    fn reduce_to_value(&self, row: &[Value], table: &Table) -> Value {
        let get_value = |name: &str| {
            table
                .columns
                .iter()
                .zip(row.iter())
                .find(|(column, _)| column.name == name)
                .map(|(_, value)| value)
        };

        match self {
            Predicate::Literal(v) => v.clone(),
            Predicate::Column(name) => get_value(name).cloned().unwrap_or(Value::Integer(0)),
            Predicate::BinaryOperator(op) => op.reduce_to_value(row, table),
            // TODO: leave this the same for now
            Predicate::Like(column, value) => get_value(column)
                .map(|v| exec_like(v.to_string().as_str(), value.as_str()).into())
                .unwrap_or(Value::FALSE),
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(v) => write!(f, "{}", v),
            Self::Column(name) => write!(f, "{}", name),
            Self::BinaryOperator(op) => write!(f, "{}", op),
            Self::Like(name, value) => write!(f, "{} LIKE '{}'", name, value),
        }
    }
}
