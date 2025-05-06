use crate::{
    generation::{
        one_of,
        table::{GTValue, LTValue},
        ArbitraryFrom,
    },
    model::{
        query::predicate::{binary_operator::BinaryOperator, Predicate},
        table::{Table, Value},
    },
};

use super::predicate::SimplePredicate;

pub struct CompoundBinaryOperator(pub BinaryOperator);
pub struct SimpleBinaryOperator(pub BinaryOperator);

impl ArbitraryFrom<(&Table, bool)> for SimpleBinaryOperator {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
        // Pick a random column
        let column_index = rng.gen_range(0..table.columns.len());
        let column = &table.columns[column_index];
        let column_values = table
            .rows
            .iter()
            .map(|r| &r[column_index])
            .collect::<Vec<_>>();
        // Pick an operator
        let operator = match predicate_value {
            true => one_of(
                vec![
                    Box::new(|rng| {
                        BinaryOperator::Eq(
                            Predicate::Column(column.name.clone()),
                            Predicate::Literal(Value::arbitrary_from(rng, &column_values)),
                        )
                    }),
                    Box::new(|rng| {
                        BinaryOperator::Gt(
                            Predicate::Column(column.name.clone()),
                            Predicate::Literal(GTValue::arbitrary_from(rng, &column_values).0),
                        )
                    }),
                    Box::new(|rng| {
                        BinaryOperator::Lt(
                            Predicate::Column(column.name.clone()),
                            Predicate::Literal(LTValue::arbitrary_from(rng, &column_values).0),
                        )
                    }),
                ],
                rng,
            ),
            false => one_of(
                vec![
                    Box::new(|rng| {
                        BinaryOperator::Neq(
                            Predicate::Column(column.name.clone()),
                            Predicate::Literal(Value::arbitrary_from(rng, &column_values)),
                        )
                    }),
                    Box::new(|rng| {
                        BinaryOperator::Gt(
                            Predicate::Column(column.name.clone()),
                            Predicate::Literal(LTValue::arbitrary_from(rng, &column_values).0),
                        )
                    }),
                    Box::new(|rng| {
                        BinaryOperator::Lt(
                            Predicate::Column(column.name.clone()),
                            Predicate::Literal(GTValue::arbitrary_from(rng, &column_values).0),
                        )
                    }),
                ],
                rng,
            ),
        };

        Self(operator)
    }
}

impl ArbitraryFrom<(&Table, bool)> for CompoundBinaryOperator {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
        // Decide if you want to create an AND or an OR
        Self(if rng.gen_bool(0.7) {
            // An AND for true requires each of its children to be true
            // An AND for false requires at least one of its children to be false
            if predicate_value {
                BinaryOperator::And(
                    SimplePredicate::arbitrary_from(rng, (table, true)).0,
                    SimplePredicate::arbitrary_from(rng, (table, true)).0,
                )
            } else {
                let b = rng.gen_bool(0.5);
                BinaryOperator::And(
                    SimplePredicate::arbitrary_from(rng, (table, false)).0,
                    SimplePredicate::arbitrary_from(rng, (table, b)).0,
                )
            }
        } else {
            // An OR for true requires at least one of its children to be true
            // An OR for false requires each of its children to be false
            if predicate_value {
                let b = rng.gen_bool(0.5);
                BinaryOperator::Or(
                    SimplePredicate::arbitrary_from(rng, (table, true)).0,
                    SimplePredicate::arbitrary_from(rng, (table, b)).0,
                )
            } else {
                BinaryOperator::And(
                    SimplePredicate::arbitrary_from(rng, (table, false)).0,
                    SimplePredicate::arbitrary_from(rng, (table, false)).0,
                )
            }
        })
    }
}

impl ArbitraryFrom<(&str, &Value)> for BinaryOperator {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, (column_name, value): (&str, &Value)) -> Self {
        one_of(
            vec![
                Box::new(|_| {
                    BinaryOperator::Eq(
                        Predicate::Column(column_name.to_string()),
                        Predicate::Literal((*value).clone()),
                    )
                }),
                Box::new(|rng| {
                    BinaryOperator::Gt(
                        Predicate::Column(column_name.to_string()),
                        Predicate::Literal(GTValue::arbitrary_from(rng, value).0),
                    )
                }),
                Box::new(|rng| {
                    BinaryOperator::Lt(
                        Predicate::Column(column_name.to_string()),
                        Predicate::Literal(LTValue::arbitrary_from(rng, value).0),
                    )
                }),
            ],
            rng,
        )
    }
}
