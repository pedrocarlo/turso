//! Lazy common-table-expression analysis.

use turso_parser::ast;

use super::{
    analyze::Analyzer,
    hir::{Cte, CteBody, CteColumn, CteId, RecursiveArm, RecursiveCte, RecursiveOrderTerm},
};
use crate::{
    translate::expr::{walk_expr, WalkControl},
    LimboError, Result,
};

pub(super) struct CteScope<'ast> {
    entries: Vec<PendingCte<'ast>>,
}

fn recursive_arm_start(syntax: &ast::CommonTableExpr) -> Result<Option<usize>> {
    let name = crate::util::normalize_ident(syntax.tbl_name.as_str());
    let arms = std::iter::once(&syntax.select.body.select)
        .chain(syntax.select.body.compounds.iter().map(|arm| &arm.select));
    let mut first_recursive = None;
    for (index, arm) in arms.enumerate() {
        let (direct, total) = recursive_references_in_arm(arm, &name);
        if total == 0 {
            if first_recursive.is_some() {
                crate::bail_parse_error!("circular reference: {}", syntax.tbl_name.as_str());
            }
            continue;
        }
        if index == 0 || direct == 0 {
            crate::bail_parse_error!("circular reference: {}", syntax.tbl_name.as_str());
        }
        if direct > 1 {
            crate::bail_parse_error!(
                "multiple references to recursive table: {}",
                syntax.tbl_name.as_str()
            );
        }
        if total > direct {
            crate::bail_parse_error!(
                "multiple recursive references: {}",
                syntax.tbl_name.as_str()
            );
        }
        first_recursive.get_or_insert(index);
    }
    let Some(first_recursive) = first_recursive else {
        return Ok(None);
    };

    let operator = syntax.select.body.compounds[first_recursive - 1].operator;
    if !matches!(
        operator,
        ast::CompoundOperator::Union | ast::CompoundOperator::UnionAll
    ) {
        crate::bail_parse_error!(
            "recursive CTEs must use UNION ALL or UNION between the initial and recursive queries"
        );
    }
    if syntax.select.body.compounds[first_recursive..]
        .iter()
        .any(|arm| arm.operator != operator)
    {
        crate::bail_parse_error!("recursive CTE queries must use the same UNION operator");
    }
    Ok(Some(first_recursive))
}

fn recursive_references_in_arm(one: &ast::OneSelect, name: &str) -> (usize, usize) {
    fn direct(table: &ast::SelectTable, name: &str) -> usize {
        match table {
            ast::SelectTable::Table(table, _, _) | ast::SelectTable::TableCall(table, _, _) => {
                usize::from(
                    table.db_name.is_none()
                        && crate::util::normalize_ident(table.name.as_str()) == name,
                )
            }
            ast::SelectTable::Sub(from, _) => {
                direct(&from.select, name)
                    + from
                        .joins
                        .iter()
                        .map(|join| direct(&join.table, name))
                        .sum::<usize>()
            }
            ast::SelectTable::Select(_, _) => 0,
        }
    }

    let ast::OneSelect::Select { from, .. } = one else {
        return (0, count_one_select_references(one, name));
    };
    let direct_count = from.as_ref().map_or(0, |from| {
        direct(&from.select, name)
            + from
                .joins
                .iter()
                .map(|join| direct(&join.table, name))
                .sum::<usize>()
    });
    (direct_count, count_one_select_references(one, name))
}

fn count_select_references(select: &ast::Select, name: &str) -> usize {
    count_one_select_references(&select.body.select, name)
        + select
            .body
            .compounds
            .iter()
            .map(|arm| count_one_select_references(&arm.select, name))
            .sum::<usize>()
}

fn count_one_select_references(one: &ast::OneSelect, name: &str) -> usize {
    fn expression(expression: &ast::Expr, name: &str) -> usize {
        let mut count = 0;
        let _ = walk_expr(expression, &mut |node| -> Result<WalkControl> {
            match node {
                ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
                    count += count_select_references(select, name);
                    Ok(WalkControl::SkipChildren)
                }
                ast::Expr::InSelect { rhs, .. } => {
                    count += count_select_references(rhs, name);
                    Ok(WalkControl::Continue)
                }
                _ => Ok(WalkControl::Continue),
            }
        });
        count
    }

    fn table(table: &ast::SelectTable, name: &str) -> usize {
        match table {
            ast::SelectTable::Table(table, _, _) => usize::from(
                table.db_name.is_none()
                    && crate::util::normalize_ident(table.name.as_str()) == name,
            ),
            ast::SelectTable::TableCall(table_name, arguments, _) => {
                usize::from(
                    table_name.db_name.is_none()
                        && crate::util::normalize_ident(table_name.name.as_str()) == name,
                ) + arguments
                    .iter()
                    .map(|argument| expression(argument, name))
                    .sum::<usize>()
            }
            ast::SelectTable::Select(select, _) => count_select_references(select, name),
            ast::SelectTable::Sub(from, _) => from_clause(from, name),
        }
    }

    fn from_clause(from: &ast::FromClause, name: &str) -> usize {
        table(&from.select, name)
            + from
                .joins
                .iter()
                .map(|join| {
                    table(&join.table, name)
                        + match &join.constraint {
                            Some(ast::JoinConstraint::On(value)) => expression(value, name),
                            _ => 0,
                        }
                })
                .sum::<usize>()
    }

    match one {
        ast::OneSelect::Values(rows) => rows
            .iter()
            .flatten()
            .map(|value| expression(value, name))
            .sum(),
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            let mut count = from.as_ref().map_or(0, |from| from_clause(from, name));
            for column in columns {
                if let ast::ResultColumn::Expr(value, _) = column {
                    count += expression(value, name);
                }
            }
            count += where_clause
                .as_deref()
                .map_or(0, |value| expression(value, name));
            if let Some(grouping) = group_by {
                count += grouping
                    .exprs
                    .iter()
                    .map(|value| expression(value, name))
                    .sum::<usize>();
                count += grouping
                    .having
                    .as_deref()
                    .map_or(0, |value| expression(value, name));
            }
            for window in window_clause {
                count += window
                    .window
                    .partition_by
                    .iter()
                    .map(|value| expression(value, name))
                    .sum::<usize>();
                count += window
                    .window
                    .order_by
                    .iter()
                    .map(|term| expression(&term.expr, name))
                    .sum::<usize>();
            }
            count
        }
    }
}

fn reject_recursive_query_functions(
    analyzer: &Analyzer<'_, '_, '_>,
    query: super::hir::QueryId,
) -> Result<()> {
    let query = analyzer
        .query(query)
        .ok_or_else(|| LimboError::InternalError(format!("missing recursive query {query}")))?;
    if query.blocks.iter().any(|block| {
        block.aggregate_count > 0
            || matches!(
                block.body,
                super::hir::QueryBlockBody::Select {
                    grouping: Some(_),
                    ..
                }
            )
    }) {
        crate::bail_parse_error!("recursive aggregate queries not supported");
    }
    if query
        .blocks
        .iter()
        .any(|block| block.window_function_count > 0)
    {
        crate::bail_parse_error!("cannot use window functions in recursive queries");
    }
    Ok(())
}

fn recursive_comparison_collations(
    analyzer: &Analyzer<'_, '_, '_>,
    seed: super::hir::QueryId,
    arms: &[RecursiveArm],
) -> Result<Vec<Option<super::hir::ResolvedCollation>>> {
    let seed = analyzer
        .query(seed)
        .ok_or_else(|| LimboError::InternalError(format!("missing recursive seed {seed}")))?;
    let mut collations = vec![None; seed.output.len()];
    let arm_queries = arms
        .iter()
        .map(|arm| {
            analyzer.query(arm.query).ok_or_else(|| {
                LimboError::InternalError(format!("missing recursive arm {}", arm.query))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    for query in std::iter::once(seed).chain(arm_queries) {
        for block in &query.blocks {
            for (collation, output) in collations.iter_mut().zip(&block.outputs) {
                if collation.is_none() {
                    *collation = output.collation.clone();
                }
            }
        }
    }
    Ok(collations)
}

struct PendingCte<'ast> {
    name: String,
    syntax: &'ast ast::CommonTableExpr,
    state: CteState,
}

enum CteState {
    Unbound,
    BindingSeed(Option<CteId>),
    BindingRecursive(RecursiveBinding),
    Bound(CteId),
    Failed(String),
}

struct RecursiveBinding {
    id: CteId,
    columns: Vec<CteColumn>,
    input_sources: Vec<super::hir::SourceId>,
}

pub(super) enum CteResolution {
    Cte(CteId),
    RecursiveInput { cte: CteId, columns: Vec<CteColumn> },
}

impl<'context, 'catalog, 'ast> Analyzer<'context, 'catalog, 'ast> {
    pub(super) fn push_cte_scope(&mut self, with: &'ast ast::With) -> Result<()> {
        let mut entries = Vec::with_capacity(with.ctes.len());
        for cte in &with.ctes {
            let name = crate::util::normalize_ident(cte.tbl_name.as_str());
            if entries
                .iter()
                .any(|entry: &PendingCte<'_>| entry.name == name)
            {
                crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
            }
            entries.push(PendingCte {
                name,
                syntax: cte,
                state: CteState::Unbound,
            });
        }
        self.cte_scopes.push(CteScope { entries });
        Ok(())
    }

    pub(super) fn resolve_cte(&mut self, name: &str) -> Result<Option<CteResolution>> {
        let name = crate::util::normalize_ident(name);
        let Some((scope, entry)) =
            self.cte_scopes
                .iter()
                .enumerate()
                .rev()
                .find_map(|(scope, entries)| {
                    entries
                        .entries
                        .iter()
                        .position(|entry| entry.name == name)
                        .map(|entry| (scope, entry))
                })
        else {
            return Ok(None);
        };

        match &self.cte_scopes[scope].entries[entry].state {
            CteState::Bound(id) => return Ok(Some(CteResolution::Cte(*id))),
            CteState::BindingSeed(_) => crate::bail_parse_error!("circular reference: {}", name),
            CteState::BindingRecursive(binding) => {
                return Ok(Some(CteResolution::RecursiveInput {
                    cte: binding.id,
                    columns: binding.columns.clone(),
                }));
            }
            CteState::Failed(message) => {
                return Err(LimboError::ParseError(message.clone()));
            }
            CteState::Unbound => {}
        }

        let syntax = self.cte_scopes[scope].entries[entry].syntax;
        let first_recursive_arm = match recursive_arm_start(syntax) {
            Ok(first_recursive_arm) => first_recursive_arm,
            Err(error) => {
                let message = match error {
                    LimboError::ParseError(message) => message,
                    other => other.to_string(),
                };
                self.cte_scopes[scope].entries[entry].state = CteState::Failed(message.clone());
                return Err(LimboError::ParseError(message));
            }
        };
        let recursive_id = first_recursive_arm.map(|_| self.reserve_cte());
        self.cte_scopes[scope].entries[entry].state = CteState::BindingSeed(recursive_id);
        let result = self.bind_cte(syntax, recursive_id, scope, entry, first_recursive_arm);
        match result {
            Ok(id) => {
                self.cte_scopes[scope].entries[entry].state = CteState::Bound(id);
                Ok(Some(CteResolution::Cte(id)))
            }
            Err(error) => {
                let message = match error {
                    LimboError::ParseError(message) => message,
                    other => other.to_string(),
                };
                self.cte_scopes[scope].entries[entry].state = CteState::Failed(message.clone());
                Err(LimboError::ParseError(message))
            }
        }
    }

    fn bind_cte(
        &mut self,
        syntax: &'ast ast::CommonTableExpr,
        recursive_id: Option<CteId>,
        scope: usize,
        entry: usize,
        first_recursive_arm: Option<usize>,
    ) -> Result<CteId> {
        if let Some(first_recursive_arm) = first_recursive_arm {
            let id = recursive_id.expect("recursive CTE reserves its identity");
            return self.bind_recursive_cte(syntax, id, scope, entry, first_recursive_arm);
        }
        let query = self.analyze_select(&syntax.select)?;
        let source_columns = self.query_source_columns(query)?;

        if !syntax.columns.is_empty() && syntax.columns.len() != source_columns.len() {
            crate::bail_parse_error!(
                "table {} has {} values for {} columns",
                syntax.tbl_name.as_str(),
                source_columns.len(),
                syntax.columns.len()
            );
        }

        let columns = source_columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| CteColumn {
                name: syntax
                    .columns
                    .get(index)
                    .map(|column| crate::util::normalize_ident(column.col_name.as_str()))
                    .unwrap_or(column.name),
                type_fact: column.type_fact,
                affinity: column.affinity,
                has_affinity: column.has_affinity,
                collation: column.collation,
            })
            .collect();
        // Allocate only referenced definitions. Closed HIR rejects arena
        // entries that cannot be reached from the statement root.
        let id = self.reserve_cte();
        self.insert_cte(
            id,
            Cte {
                id,
                name: crate::util::normalize_ident(syntax.tbl_name.as_str()),
                columns,
                materialized: syntax.materialized.clone(),
                body: CteBody::Query(query),
            },
        )?;
        Ok(id)
    }

    fn bind_recursive_cte(
        &mut self,
        syntax: &'ast ast::CommonTableExpr,
        id: CteId,
        scope: usize,
        entry: usize,
        first_recursive_arm: usize,
    ) -> Result<CteId> {
        let seed_compounds = &syntax.select.body.compounds[..first_recursive_arm - 1];
        let seed = self.analyze_cte_query_parts(
            &syntax.select,
            &syntax.select.body.select,
            seed_compounds,
        )?;
        let source_columns = self.query_source_columns(seed)?;
        if !syntax.columns.is_empty() && syntax.columns.len() != source_columns.len() {
            crate::bail_parse_error!(
                "table {} has {} values for {} columns",
                syntax.tbl_name.as_str(),
                source_columns.len(),
                syntax.columns.len()
            );
        }
        let columns = source_columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| CteColumn {
                name: syntax
                    .columns
                    .get(index)
                    .map(|column| crate::util::normalize_ident(column.col_name.as_str()))
                    .unwrap_or(column.name),
                type_fact: column.type_fact,
                affinity: column.affinity,
                has_affinity: column.has_affinity,
                collation: column.collation,
            })
            .collect::<Vec<_>>();

        match &self.cte_scopes[scope].entries[entry].state {
            CteState::BindingSeed(Some(binding_id)) if *binding_id == id => {}
            _ => {
                return Err(LimboError::InternalError(format!(
                    "recursive CTE {id} left seed-binding state"
                )));
            }
        }
        self.cte_scopes[scope].entries[entry].state =
            CteState::BindingRecursive(RecursiveBinding {
                id,
                columns: columns.clone(),
                input_sources: Vec::new(),
            });

        let mut arms = Vec::new();
        for compound in &syntax.select.body.compounds[first_recursive_arm - 1..] {
            let query = self.analyze_cte_query_parts(&syntax.select, &compound.select, &[])?;
            reject_recursive_query_functions(self, query)?;
            let width = self.query_source_columns(query)?.len();
            if width != columns.len() {
                crate::bail_parse_error!(
                    "SELECTs to the left and right of {} do not have the same number of result columns",
                    compound.operator
                );
            }
            arms.push(RecursiveArm {
                operator: compound.operator,
                query,
            });
        }

        let input_sources = match &mut self.cte_scopes[scope].entries[entry].state {
            CteState::BindingRecursive(binding) if binding.id == id => {
                std::mem::take(&mut binding.input_sources)
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "recursive CTE {id} left recursive-binding state"
                )));
            }
        };
        let comparison_collations = recursive_comparison_collations(self, seed, &arms)?;
        let queue_order = self.analyze_recursive_order_by(&syntax.select.order_by, seed, &arms)?;
        let limit = syntax
            .select
            .limit
            .as_ref()
            .map(|limit| self.analyze_limit(limit, seed))
            .transpose()?;
        self.insert_cte(
            id,
            Cte {
                id,
                name: crate::util::normalize_ident(syntax.tbl_name.as_str()),
                columns,
                materialized: syntax.materialized.clone(),
                body: CteBody::Recursive(RecursiveCte {
                    seed,
                    arms,
                    input_sources,
                    comparison_collations,
                    queue_order,
                    limit,
                }),
            },
        )?;
        Ok(id)
    }

    fn analyze_recursive_order_by(
        &mut self,
        syntax: &'ast [ast::SortedColumn],
        seed: super::hir::QueryId,
        arms: &[RecursiveArm],
    ) -> Result<Vec<RecursiveOrderTerm>> {
        let positions = {
            let seed = self.query(seed).ok_or_else(|| {
                LimboError::InternalError(format!("missing recursive seed {seed}"))
            })?;
            let arm_queries = arms
                .iter()
                .map(|arm| {
                    self.query(arm.query).ok_or_else(|| {
                        LimboError::InternalError(format!("missing recursive arm {}", arm.query))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let output_arms = std::iter::once(seed)
                .chain(arm_queries)
                .flat_map(|query| query.blocks.iter().map(|block| block.outputs.as_slice()))
                .collect::<Vec<_>>();
            syntax
                .iter()
                .enumerate()
                .map(|(index, term)| {
                    super::analyze::compound_order_by_position(&term.expr, &output_arms, index + 1)
                })
                .collect::<Result<Vec<_>>>()?
        };

        syntax
            .iter()
            .zip(positions)
            .map(|(syntax, (output, collations))| {
                let mut explicit_collation = None;
                for name in collations.iter().rev() {
                    explicit_collation = Some(self.resolve_collation(name)?);
                }
                Ok(RecursiveOrderTerm {
                    output,
                    order: syntax.order.unwrap_or(ast::SortOrder::Asc),
                    nulls: syntax.nulls,
                    explicit_collation,
                })
            })
            .collect()
    }

    fn analyze_cte_query_parts(
        &mut self,
        whole: &'ast ast::Select,
        first: &'ast ast::OneSelect,
        compounds: &'ast [ast::CompoundSelect],
    ) -> Result<super::hir::QueryId> {
        if let Some(with) = &whole.with {
            self.push_cte_scope(with)?;
        }
        let result = self.analyze_select_parts(first, compounds, &[], None, None, None);
        if whole.with.is_some() {
            self.cte_scopes
                .pop()
                .expect("recursive CTE body WITH scope was pushed");
        }
        result
    }

    pub(super) fn record_recursive_input(
        &mut self,
        cte: CteId,
        source: super::hir::SourceId,
    ) -> Result<()> {
        for scope in self.cte_scopes.iter_mut().rev() {
            for entry in &mut scope.entries {
                if let CteState::BindingRecursive(binding) = &mut entry.state {
                    if binding.id == cte {
                        binding.input_sources.push(source);
                        return Ok(());
                    }
                }
            }
        }
        Err(LimboError::InternalError(format!(
            "recursive input {source} has no active CTE {cte}"
        )))
    }
}
