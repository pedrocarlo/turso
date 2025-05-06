use std::fmt::Display;

use super::{Predicate, TestPredicate};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BinaryOperator {
    And(Predicate, Predicate),
    Or(Predicate, Predicate),
    Eq(Predicate, Predicate),
    Neq(Predicate, Predicate),
    Gt(Predicate, Predicate),
    Lt(Predicate, Predicate),
}

impl TestPredicate for BinaryOperator {
    fn test(&self, row: &[crate::model::table::Value], table: &crate::model::table::Table) -> bool {
        self.reduce_to_value(row, table).into()
    }

    fn reduce_to_value(
        &self,
        row: &[crate::model::table::Value],
        table: &crate::model::table::Table,
    ) -> crate::model::table::Value {
        match self {
            BinaryOperator::And(lhs, rhs) => {
                let lhs: bool = lhs.reduce_to_value(row, table).into();
                let rhs: bool = rhs.reduce_to_value(row, table).into();
                (lhs && rhs).into()
            }
            BinaryOperator::Or(lhs, rhs) => {
                let lhs: bool = lhs.reduce_to_value(row, table).into();
                let rhs: bool = rhs.reduce_to_value(row, table).into();
                (lhs || rhs).into()
            }
            BinaryOperator::Eq(lhs, rhs) => {
                let lhs = lhs.reduce_to_value(row, table);
                let rhs = rhs.reduce_to_value(row, table);
                (lhs == rhs).into()
            }
            BinaryOperator::Neq(lhs, rhs) => {
                let lhs = lhs.reduce_to_value(row, table);
                let rhs = rhs.reduce_to_value(row, table);
                (lhs != rhs).into()
            }
            BinaryOperator::Gt(lhs, rhs) => {
                let lhs = lhs.reduce_to_value(row, table);
                let rhs = rhs.reduce_to_value(row, table);
                (lhs > rhs).into()
            }
            BinaryOperator::Lt(lhs, rhs) => {
                let lhs = lhs.reduce_to_value(row, table);
                let rhs = rhs.reduce_to_value(row, table);
                (lhs < rhs).into()
            }
        }
    }
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (lhs, rhs) = match self {
            BinaryOperator::And(lhs, rhs)
            | BinaryOperator::Or(lhs, rhs)
            | BinaryOperator::Eq(lhs, rhs)
            | BinaryOperator::Neq(lhs, rhs)
            | BinaryOperator::Gt(lhs, rhs)
            | BinaryOperator::Lt(lhs, rhs) => (lhs.to_string(), rhs.to_string()),
        };
        match self {
            BinaryOperator::And(..) => write!(f, "{} AND {}", lhs, rhs),
            BinaryOperator::Or(..) => write!(f, "{} OR {}", lhs, rhs),
            BinaryOperator::Eq(..) => write!(f, "{} = {}", lhs, rhs),
            BinaryOperator::Neq(..) => write!(f, "{} != {}", lhs, rhs),
            BinaryOperator::Gt(..) => write!(f, "{} > {}", lhs, rhs),
            BinaryOperator::Lt(..) => write!(f, "{} < {}", lhs, rhs),
        }
    }
}
