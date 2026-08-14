//! Adapts resolved semantic sources to planner structures.

use super::{
    optimizer::{
        constraints::{hir_table_constraints_for_source, HirTableConstraints},
        CostModelParams,
    },
    plan::{ColumnUsedMask, HirJoinInfo, HirPlannedSource, HirWhereTerm, Operation, PredicateExpr},
    semantic::hir::{self, ColumnUsage, HirDocument, QueryId, SourceId},
};
use crate::{schema::Schema, LimboError, Result};

/// Resolved source and predicate input for one query block.
pub(crate) struct HirQueryBlockPlanInput {
    pub(crate) sources: Vec<HirPlannedSource>,
    pub(crate) predicates: Vec<HirWhereTerm>,
}

/// State shared while one HIR document is converted into plan nodes.
pub(crate) struct HirPlanContext<'a> {
    document: &'a HirDocument,
}

impl<'a> HirPlanContext<'a> {
    pub(crate) fn new(document: &'a HirDocument) -> Self {
        Self { document }
    }

    fn definition(&self, id: SourceId) -> &'a hir::Source {
        self.document
            .source(id)
            .expect("validated HIR contains referenced source")
    }

    /// Build planner source metadata without resolving names, allocating a
    /// second identity, or reading the catalog again.
    pub(crate) fn source(
        &self,
        source: SourceId,
        join_info: Option<HirJoinInfo>,
        usage: &[ColumnUsage],
    ) -> Result<HirPlannedSource> {
        planned_source_from_definition(self.definition(source), join_info, usage)
    }

    /// Assemble planner input from resolved HIR without repeating binding.
    pub(crate) fn query_block_input(
        &self,
        block: &hir::QueryBlock,
        order_by: &[hir::OrderTerm],
        usage: &[ColumnUsage],
    ) -> Result<HirQueryBlockPlanInput> {
        let mut input = HirQueryBlockPlanInput {
            sources: Vec::new(),
            predicates: Vec::new(),
        };

        if let Some(from) = &block.from {
            input.sources.push(self.source(from.first, None, usage)?);
            for join in &from.joins {
                let using = match &join.constraint {
                    hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                        columns.iter().map(|column| column.name.clone()).collect()
                    }
                    hir::JoinConstraint::None | hir::JoinConstraint::On(_) => Vec::new(),
                };
                input.sources.push(self.source(
                    join.right,
                    Some(HirJoinInfo {
                        kind: join.kind,
                        using,
                    }),
                    usage,
                )?);

                let from_outer_join = match join.kind {
                    hir::JoinKind::Left | hir::JoinKind::Right | hir::JoinKind::Full => {
                        Some(join.right)
                    }
                    hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross => None,
                };
                match &join.constraint {
                    hir::JoinConstraint::None => {}
                    hir::JoinConstraint::On(expression) => {
                        append_predicates(&mut input.predicates, expression, from_outer_join)
                    }
                    hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                        for column in columns {
                            input.predicates.push(HirWhereTerm {
                                expr: using_equality(column),
                                from_outer_join,
                                consumed: false,
                            });
                        }
                    }
                }
            }
        }

        if let hir::QueryBlockBody::Select {
            filter: Some(filter),
            ..
        } = &block.body
        {
            append_predicates(&mut input.predicates, filter, None);
        }

        for output in &block.outputs {
            self.register_expression_index_usage(&mut input.sources, &output.expr)?;
        }
        if let hir::QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } = &block.body
        {
            for key in &grouping.keys {
                self.register_expression_index_usage(&mut input.sources, key)?;
            }
            if let Some(having) = &grouping.having {
                self.register_expression_index_usage(&mut input.sources, having)?;
            }
        }
        for term in order_by {
            self.register_expression_index_usage(&mut input.sources, &term.expr)?;
        }

        Ok(input)
    }

    /// Collect physical constraints for every source while keeping the source
    /// and predicate order established by `query_block_input`.
    pub(crate) fn constraints(
        &self,
        from: &hir::From,
        input: &HirQueryBlockPlanInput,
        schema: &Schema,
        params: &CostModelParams,
        query_rows: &dyn Fn(QueryId) -> Option<f64>,
    ) -> Result<Vec<HirTableConstraints>> {
        input
            .sources
            .iter()
            .map(|source| {
                hir_table_constraints_for_source(
                    self.document,
                    from,
                    &input.predicates,
                    source,
                    schema,
                    params,
                    query_rows,
                )
            })
            .collect()
    }

    fn register_expression_index_usage(
        &self,
        sources: &mut [HirPlannedSource],
        expression: &hir::Expr,
    ) -> Result<()> {
        let Some((source, columns)) = expression.single_source_column_usage() else {
            return Ok(());
        };
        let Some(planned) = sources
            .iter_mut()
            .find(|planned| planned.internal_id == source)
        else {
            // Outer-query reads are planned by their owning query.
            return Ok(());
        };
        let has_expression_key = self
            .definition(source)
            .index_expressions
            .iter()
            .any(|index| index.columns.iter().any(Option::is_some));
        if !has_expression_key {
            return Ok(());
        }

        let mut columns_mask = ColumnUsedMask::default();
        for column in columns {
            columns_mask.set(column)?;
        }
        planned.register_expression_index_usage(expression.clone(), columns_mask);
        Ok(())
    }
}

fn append_predicates(
    predicates: &mut Vec<HirWhereTerm>,
    expression: &hir::Expr,
    from_outer_join: Option<SourceId>,
) {
    predicates.extend(expression.conjuncts().map(|expression| HirWhereTerm {
        expr: expression.clone(),
        from_outer_join,
        consumed: false,
    }));
}

fn using_equality(column: &hir::UsingColumn) -> hir::Expr {
    hir::Expr::Binary {
        lhs: column.left.clone(),
        operator: turso_parser::ast::Operator::Equals,
        rhs: Box::new(hir::Expr::Column(column.right)),
        array_concat: false,
        custom: None,
        comparison: Some(column.comparison.clone()),
    }
}

fn planned_source_from_definition(
    source: &hir::Source,
    join_info: Option<HirJoinInfo>,
    usage: &[ColumnUsage],
) -> Result<HirPlannedSource> {
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(format!(
            "source {} is not a table source",
            source.id
        )));
    };
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!("table source {} has no database", source.id))
    })?;
    let (col_used_mask, column_use_counts) = column_usage(source.id, usage)?;

    Ok(HirPlannedSource {
        op: Operation::default_scan_for(table.value()),
        table: table.value().clone(),
        identifier: source.alias.as_ref().unwrap_or(&source.name).clone(),
        internal_id: source.id,
        join_info,
        col_used_mask,
        column_use_counts,
        expression_index_usages: Vec::new(),
        database_id: database.index(),
        indexed: source.index_hint.clone(),
    })
}

fn column_usage(source: SourceId, usage: &[ColumnUsage]) -> Result<(ColumnUsedMask, Vec<usize>)> {
    let mut mask = ColumnUsedMask::default();
    let mut counts = Vec::new();
    for usage in usage
        .iter()
        .filter(|usage| usage.reference.source == source)
    {
        mask.set(usage.reference.column)?;
        if counts.len() <= usage.reference.column {
            counts.resize(usage.reference.column + 1, 0);
        }
        counts[usage.reference.column] = usage.count;
    }
    Ok((mask, counts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        schema::{BTreeCharacteristics, BTreeTable, ColDef, Column, Index, Schema, Table, Type},
        sync::Arc,
        translate::semantic::hir::{
            CatalogObject, CatalogObjectId, CatalogSnapshot, ColumnReadExpression,
            ComparisonComponent, ComparisonSemantics, DatabaseId, IndexCoverage, Join,
            JoinConstraint, JoinKind, QueryBlock, QueryBlockBody, QueryBlockId, QueryId,
            SourceColumn, SourceKind, SourceOwner, TypeFact, UsingColumn,
        },
        vdbe::affinity::Affinity,
    };
    use turso_parser::ast::Operator;

    fn resolved_table(name: &str) -> hir::ResolvedTable {
        let columns = vec![
            Column::new(
                Some("first".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef::default(),
            ),
            Column::new(
                Some("second".to_string()),
                "TEXT".to_string(),
                None,
                None,
                Type::Text,
                None,
                ColDef::default(),
            ),
        ];
        let table = Table::BTree(Arc::new(BTreeTable::new(
            2,
            name.to_string(),
            vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        )));
        CatalogObject::new(
            CatalogObjectId::new(10),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(table),
        )
    }

    fn source() -> hir::Source {
        hir::Source {
            id: SourceId::new(0),
            owner: SourceOwner::Root,
            database: Some(DatabaseId::new(0)),
            name: "items".to_string(),
            alias: Some("i".to_string()),
            kind: SourceKind::Table(resolved_table("items")),
            columns: vec![
                SourceColumn {
                    name: "first".to_string(),
                    type_fact: TypeFact::known(Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                    hidden: false,
                    rowid_alias: false,
                },
                SourceColumn {
                    name: "second".to_string(),
                    type_fact: TypeFact::known(Type::Text),
                    affinity: Affinity::Text,
                    has_affinity: true,
                    collation: None,
                    hidden: false,
                    rowid_alias: false,
                },
            ],
            generated_expressions: vec![ColumnReadExpression::Absent; 2],
            default_expressions: vec![ColumnReadExpression::Absent; 2],
            column_type_programs: vec![None; 2],
            check_constraints: None,
            rowid_available: true,
            index_hint: hir::IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    fn source_with_id(id: usize, name: &str) -> hir::Source {
        let mut source = source();
        source.id = SourceId::new(id);
        source.name = name.to_string();
        source.alias = None;
        source.kind = SourceKind::Table(resolved_table(name));
        source
    }

    fn document(sources: Vec<hir::Source>) -> hir::HirDocument {
        hir::HirDocument {
            snapshot: CatalogSnapshot::from_id(1),
            databases: Vec::new(),
            root: hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
                source: sources[0].id,
                expressions: Vec::new(),
            }),
            queries: Vec::new(),
            sources,
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        }
    }

    fn binary(lhs: hir::Expr, operator: Operator, rhs: hir::Expr) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(lhs),
            operator,
            rhs: Box::new(rhs),
            array_concat: false,
            custom: None,
            comparison: None,
        }
    }

    #[test]
    fn hir_source_reuses_identity_metadata_and_alias() {
        let source = source();
        let SourceKind::Table(resolved) = &source.kind else {
            unreachable!();
        };
        let document = hir::HirDocument {
            snapshot: CatalogSnapshot::from_id(1),
            databases: Vec::new(),
            root: hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
                source: source.id,
                expressions: Vec::new(),
            }),
            queries: Vec::new(),
            sources: vec![source.clone()],
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        };
        let context = HirPlanContext::new(&document);
        let joined = context
            .source(
                source.id,
                Some(HirJoinInfo {
                    kind: hir::JoinKind::Inner,
                    using: vec!["first".into()],
                }),
                &[],
            )
            .expect("table source converts");

        assert_eq!(joined.identifier, "i");
        assert_eq!(joined.internal_id, source.id);
        assert_eq!(joined.join_info.as_ref().unwrap().using[0], "first");
        assert_eq!(joined.database_id, 0);
        let (Table::BTree(expected), Table::BTree(actual)) = (resolved.value(), &joined.table)
        else {
            panic!("resolved and planned tables are btree tables");
        };
        assert!(Arc::ptr_eq(expected, actual));

        let mut unaliased = source;
        unaliased.alias = None;
        let joined = planned_source_from_definition(&unaliased, None, &[])
            .expect("unaliased table source converts");
        assert_eq!(joined.identifier, "items");
    }

    #[test]
    fn table_source_uses_hir_column_counts() {
        let source = source();
        let usage = [ColumnUsage {
            reference: hir::ColumnRef {
                source: source.id,
                column: 1,
            },
            count: 3,
        }];
        let joined =
            planned_source_from_definition(&source, None, &usage).expect("table source converts");

        assert_eq!(
            (&joined.col_used_mask).into_iter().collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(joined.column_use_counts, vec![0, 3]);
    }

    #[test]
    fn table_source_preserves_index_hints_without_lookup() {
        let mut source = source();
        source.index_hint = hir::IndexHint::NotIndexed;
        let joined =
            planned_source_from_definition(&source, None, &[]).expect("table source converts");
        assert!(matches!(joined.indexed, hir::IndexHint::NotIndexed));

        let index = Index {
            name: "items_second".to_string(),
            table_name: "items".to_string(),
            root_page: 3,
            columns: Vec::new(),
            unique: false,
            ephemeral: false,
            has_rowid: true,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        };
        source.index_hint = hir::IndexHint::Indexed(CatalogObject::new(
            CatalogObjectId::new(11),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(index),
        ));
        let joined =
            planned_source_from_definition(&source, None, &[]).expect("table source converts");
        assert!(matches!(
            joined.indexed,
            hir::IndexHint::Indexed(index) if index.value().name == "items_second"
        ));
    }

    #[test]
    fn hir_covering_index_matches_frozen_expression_keys() {
        let mut source = source();
        let indexed_expression = binary(
            hir::Expr::column(source.id, 0),
            Operator::Add,
            hir::Expr::column(source.id, 1),
        );
        let index = CatalogObject::new(
            CatalogObjectId::new(11),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(Index {
                name: "items_sum".to_string(),
                table_name: "items".to_string(),
                root_page: 3,
                columns: Vec::new(),
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }),
        );
        source.index_expressions.push(hir::IndexExpressions {
            index: index.clone(),
            columns: vec![Some(indexed_expression.clone())],
            predicate: None,
        });

        let usage = [
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: source.id,
                    column: 0,
                },
                count: 1,
            },
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: source.id,
                    column: 1,
                },
                count: 1,
            },
        ];
        let block_id = QueryBlockId::new(QueryId::new(0), 0);
        let mut block = QueryBlock::new(
            block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: source.id,
            joins: Vec::new(),
        });
        block.outputs.push(hir::Output {
            id: hir::OutputId::query(block_id, 0),
            name: "sum".to_string(),
            expr: indexed_expression.clone(),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: true,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        });
        let order_by = [hir::OrderTerm {
            expr: indexed_expression,
            order: turso_parser::ast::SortOrder::Asc,
            nulls: None,
            type_fact: TypeFact::known(Type::Integer),
            collation: None,
        }];
        let document = document(vec![source.clone()]);
        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &order_by, &usage)
            .expect("query block input converts");
        let mut planned = input.sources.into_iter().next().expect("source is planned");

        // Output and ORDER BY use the same expression, so registration deduplicates them.
        assert_eq!(planned.expression_index_usages.len(), 1);

        assert!(planned.index_is_covering(&source, index.value()));

        planned.expression_index_usages[0].normalized_expr =
            Box::new(hir::Expr::column(source.id, 0));
        assert!(!planned.index_is_covering(&source, index.value()));
    }

    #[test]
    fn non_table_source_is_rejected() {
        let mut source = source();
        source.kind = SourceKind::SchemaExpression;
        let error = planned_source_from_definition(&source, None, &[])
            .expect_err("schema namespace has no planner table");
        assert_eq!(
            error.to_string(),
            "Internal error: source s0 is not a table source"
        );
    }

    #[test]
    fn query_block_input_preserves_source_order_and_right_join_predicates() {
        let left = SourceId::new(0);
        let right = SourceId::new(1);
        let document = document(vec![
            source_with_id(0, "left_items"),
            source_with_id(1, "right_items"),
        ]);
        let on = binary(
            binary(
                hir::Expr::column(left, 0),
                Operator::Equals,
                hir::Expr::column(right, 0),
            ),
            Operator::And,
            binary(
                hir::Expr::column(left, 1),
                Operator::Equals,
                hir::Expr::column(right, 1),
            ),
        );
        let filter = binary(
            hir::Expr::column(right, 0),
            Operator::NotEquals,
            hir::Expr::column(right, 1),
        );
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(filter),
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: left,
            joins: vec![Join {
                right,
                kind: JoinKind::Right,
                constraint: JoinConstraint::On(on),
            }],
        });

        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &[], &[])
            .expect("query block input converts");

        assert_eq!(
            input
                .sources
                .iter()
                .map(|source| source.internal_id)
                .collect::<Vec<_>>(),
            vec![left, right]
        );
        assert!(input.sources[0].join_info.is_none());
        let join = input.sources[1].join_info.as_ref().unwrap();
        assert_eq!(join.kind, JoinKind::Right);
        assert!(join.using.is_empty());
        assert_eq!(input.predicates.len(), 3);
        assert!(input.predicates[..2]
            .iter()
            .all(|predicate| predicate.from_outer_join == Some(right)));
        assert_eq!(input.predicates[2].from_outer_join, None);
        assert!(matches!(
            &input.predicates[0].expr,
            hir::Expr::Binary {
                operator: Operator::Equals,
                ..
            }
        ));
        assert!(matches!(
            &input.predicates[1].expr,
            hir::Expr::Binary {
                operator: Operator::Equals,
                ..
            }
        ));
        assert!(matches!(
            &input.predicates[2].expr,
            hir::Expr::Binary {
                operator: Operator::NotEquals,
                ..
            }
        ));
    }

    #[test]
    fn query_block_input_builds_using_equality_from_resolved_columns() {
        let left = SourceId::new(0);
        let right = SourceId::new(1);
        let document = document(vec![
            source_with_id(0, "left_items"),
            source_with_id(1, "right_items"),
        ]);
        let comparison = ComparisonSemantics {
            components: vec![ComparisonComponent {
                affinity: Affinity::Integer,
                collation: None,
                array: false,
            }],
        };
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: left,
            joins: vec![Join {
                right,
                kind: JoinKind::Left,
                constraint: JoinConstraint::Using(vec![UsingColumn {
                    name: "first".to_string(),
                    left: Box::new(hir::Expr::column(left, 0)),
                    right: hir::ColumnRef {
                        source: right,
                        column: 0,
                    },
                    value: hir::MergedColumnValue::Left,
                    type_fact: TypeFact::known(Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                    comparison: comparison.clone(),
                }]),
            }],
        });

        let context = HirPlanContext::new(&document);
        let input = context
            .query_block_input(&block, &[], &[])
            .expect("query block input converts");

        assert_eq!(
            input.sources[1].join_info.as_ref().unwrap().using,
            ["first"]
        );
        assert_eq!(input.predicates.len(), 1);
        assert_eq!(input.predicates[0].from_outer_join, Some(right));
        let hir::Expr::Binary {
            lhs,
            operator,
            rhs,
            comparison: actual_comparison,
            ..
        } = &input.predicates[0].expr
        else {
            panic!("USING becomes equality predicate");
        };
        assert_eq!(*operator, Operator::Equals);
        assert!(matches!(
            lhs.as_ref(),
            hir::Expr::Column(column) if *column == hir::ColumnRef { source: left, column: 0 }
        ));
        assert!(matches!(
            rhs.as_ref(),
            hir::Expr::Column(column) if *column == hir::ColumnRef { source: right, column: 0 }
        ));
        assert_eq!(actual_comparison.as_ref(), Some(&comparison));

        let constraints = context
            .constraints(
                block.from.as_ref().expect("test block has FROM"),
                &input,
                &Schema::default(),
                &CostModelParams::default(),
                &|_| None,
            )
            .expect("whole query block constraints collect");
        assert_eq!(constraints.len(), 2);
        assert_eq!(constraints[0].table_id, left);
        assert_eq!(constraints[1].table_id, right);
        assert!(!constraints[1].constraints.is_empty());
    }
}
