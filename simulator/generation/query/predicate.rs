use rand::{seq::SliceRandom as _, Rng};

use crate::{
    generation::{
        backtrack, one_of,
        table::{GTValue, LTValue, LikeValue},
        ArbitraryFrom, ArbitraryFromMaybe as _,
    },
    model::{
        query::predicate::{binary_operator::BinaryOperator, Predicate},
        table::{Table, Value},
    },
};

use super::binary_operator::{CompoundBinaryOperator, SimpleBinaryOperator};

pub struct CompoundPredicate(pub Predicate);
pub struct SimplePredicate(pub Predicate);

impl ArbitraryFrom<(&Table, bool)> for SimplePredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
        // Pick an operator
        let operator = SimpleBinaryOperator::arbitrary_from(rng, (table, predicate_value));

        Self(Predicate::BinaryOperator(Box::new(operator.0)))
    }
}

impl ArbitraryFrom<(&Table, bool)> for CompoundPredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
        // Decide if you want to create an AND or an OR
        Self(Predicate::BinaryOperator(Box::new(
            CompoundBinaryOperator::arbitrary_from(rng, (table, predicate_value)).0,
        )))
    }
}

impl ArbitraryFrom<&Table> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        let predicate_value = rng.gen_bool(0.5);
        CompoundPredicate::arbitrary_from(rng, (table, predicate_value)).0
    }
}

impl ArbitraryFrom<(&str, &Value)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (column_name, value): (&str, &Value)) -> Self {
        Self::BinaryOperator(Box::new(BinaryOperator::arbitrary_from(
            rng,
            (column_name, value),
        )))
    }
}

/// Produces a predicate that is true for the provided row in the given table
fn produce_true_predicate<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Predicate {
    // Pick a column
    let column_index = rng.gen_range(0..t.columns.len());
    let column = &t.columns[column_index];
    let value = &row[column_index];
    backtrack(
        vec![
            (
                1,
                Box::new(|_| {
                    Some(Predicate::BinaryOperator(Box::new(BinaryOperator::Eq(
                        Predicate::Column(column.name.clone()),
                        Predicate::Literal(value.clone()),
                    ))))
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    let v = Value::arbitrary_from(rng, &column.column_type);
                    if &v == value {
                        None
                    } else {
                        Some(Predicate::BinaryOperator(Box::new(BinaryOperator::Neq(
                            Predicate::Column(column.name.clone()),
                            Predicate::Literal(v),
                        ))))
                    }
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    Some(Predicate::BinaryOperator(Box::new(BinaryOperator::Gt(
                        Predicate::Column(column.name.clone()),
                        Predicate::Literal(LTValue::arbitrary_from(rng, value).0),
                    ))))
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    Some(Predicate::BinaryOperator(Box::new(BinaryOperator::Lt(
                        Predicate::Column(column.name.clone()),
                        Predicate::Literal(GTValue::arbitrary_from(rng, value).0),
                    ))))
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    LikeValue::arbitrary_from_maybe(rng, value)
                        .map(|like| Predicate::Like(column.name.clone(), like.0))
                }),
            ),
        ],
        rng,
    )
}

/// Produces a predicate that is false for the provided row in the given table
fn produce_false_predicate<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Predicate {
    // Pick a column
    let column_index = rng.gen_range(0..t.columns.len());
    let column = &t.columns[column_index];
    let value = &row[column_index];
    one_of(
        vec![
            Box::new(|_| {
                Predicate::BinaryOperator(Box::new(BinaryOperator::Neq(
                    Predicate::Column(column.name.clone()),
                    Predicate::Literal(value.clone()),
                )))
            }),
            Box::new(|rng| {
                let v = loop {
                    let v = Value::arbitrary_from(rng, &column.column_type);
                    if &v != value {
                        break v;
                    }
                };
                Predicate::BinaryOperator(Box::new(BinaryOperator::Eq(
                    Predicate::Column(column.name.clone()),
                    Predicate::Literal(v),
                )))
            }),
            Box::new(|rng| {
                Predicate::BinaryOperator(Box::new(BinaryOperator::Gt(
                    Predicate::Column(column.name.clone()),
                    Predicate::Literal(GTValue::arbitrary_from(rng, value).0),
                )))
            }),
            Box::new(|rng| {
                Predicate::BinaryOperator(Box::new(BinaryOperator::Lt(
                    Predicate::Column(column.name.clone()),
                    Predicate::Literal(LTValue::arbitrary_from(rng, value).0),
                )))
            }),
        ],
        rng,
    )
}

impl ArbitraryFrom<(&Table, &Vec<Value>)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Self {
        // We want to produce a predicate that is true for the row
        // We can do this by creating several predicates that
        // are true, some that are false, combiend them in ways that correspond to the creation of a true predicate

        // Produce some true and false predicates
        let mut true_predicates = (1..=rng.gen_range(1..=4))
            .map(|_| produce_true_predicate(rng, (t, row)))
            .collect::<Vec<_>>();

        let false_predicates = (0..=rng.gen_range(0..=3))
            .map(|_| produce_false_predicate(rng, (t, row)))
            .collect::<Vec<_>>();

        // Start building a top level predicate from a true predicate
        let mut result = true_predicates.pop().unwrap();

        let mut predicates = true_predicates
            .iter()
            .map(|p| (true, p.clone()))
            .chain(false_predicates.iter().map(|p| (false, p.clone())))
            .collect::<Vec<_>>();

        predicates.shuffle(rng);

        while !predicates.is_empty() {
            // Create a new predicate from at least 1 and at most 3 predicates
            let context =
                predicates[0..rng.gen_range(0..=usize::min(3, predicates.len()))].to_vec();
            // Shift `predicates` to remove the predicates in the context
            predicates = predicates[context.len()..].to_vec();

            // `result` is true, so we have the following three options to make a true predicate:
            // T or F
            // T or T
            // T and T
            result = one_of(
                vec![
                    // T or (X1 or X2 or ... or Xn)
                    Box::new(|_| {
                        Predicate::BinaryOperator(Box::new(BinaryOperator::Or(
                            result.clone(),
                            context
                                .iter()
                                .map(|(_, p)| p.clone())
                                .reduce(|acc, p| {
                                    Predicate::BinaryOperator(Box::new(BinaryOperator::Or(acc, p)))
                                })
                                .unwrap_or(Predicate::Literal(Value::FALSE)),
                        )))
                    }),
                    // T or (T1 and T2 and ... and Tn)
                    Box::new(|_| {
                        Predicate::BinaryOperator(Box::new(BinaryOperator::Or(
                            result.clone(),
                            context
                                .iter()
                                .map(|(_, p)| p.clone())
                                .reduce(|acc, p| {
                                    Predicate::BinaryOperator(Box::new(BinaryOperator::And(acc, p)))
                                })
                                .unwrap_or(Predicate::Literal(Value::FALSE)),
                        )))
                    }),
                    // T and T
                    Box::new(|_| {
                        // Check if all the predicates in the context are true
                        if context.iter().all(|(b, _)| *b) {
                            // T and (X1 or X2 or ... or Xn)
                            Predicate::BinaryOperator(Box::new(BinaryOperator::And(
                                result.clone(),
                                context
                                    .iter()
                                    .map(|(_, p)| p.clone())
                                    .reduce(|acc, p| {
                                        Predicate::BinaryOperator(Box::new(BinaryOperator::And(
                                            acc, p,
                                        )))
                                    })
                                    .unwrap_or(Predicate::Literal(Value::TRUE)),
                            )))
                        }
                        // Check if there is at least one true predicate
                        else if context.iter().any(|(b, _)| *b) {
                            // T and (X1 or X2 or ... or Xn)
                            Predicate::BinaryOperator(Box::new(BinaryOperator::And(
                                result.clone(),
                                context
                                    .iter()
                                    .map(|(_, p)| p.clone())
                                    .reduce(|acc, p| {
                                        Predicate::BinaryOperator(Box::new(BinaryOperator::Or(
                                            acc, p,
                                        )))
                                    })
                                    .unwrap_or(Predicate::Literal(Value::TRUE)),
                            )))
                        } else {
                            // T and (X1 or X2 or ... or Xn or TRUE)
                            Predicate::BinaryOperator(Box::new(BinaryOperator::And(
                                result.clone(),
                                context
                                    .iter()
                                    .map(|(_, p)| p.clone())
                                    .chain(std::iter::once(Predicate::true_()))
                                    .reduce(|acc, p| {
                                        Predicate::BinaryOperator(Box::new(BinaryOperator::Or(
                                            acc, p,
                                        )))
                                    })
                                    .unwrap(),
                            )))
                        }
                    }),
                ],
                rng,
            );
        }

        result
    }
}
