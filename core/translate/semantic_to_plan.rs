//! Adapts resolved semantic sources to planner structures.

use super::{
    optimizer::{
        apply_hir_selected_btree_access, base_row_estimate,
        constraints::{hir_constraints_for_source, HirConstraintSource, HirTableConstraints},
        cost::{Cost, RowCountEstimate},
        join::compute_hir_greedy_join_order,
        order::{
            ColumnOrder, ColumnTarget, EliminatesSortBy, HirOrderTarget, OrderTarget,
            OrderTargetPurpose,
        },
        CostModelParams, HirBtreeOperation,
    },
    plan::{
        ColumnUsedMask, HirCteMaterialization, HirJoinInfo, HirPlanSource, HirPlannedSource,
        HirWhereTerm, PredicateExpr,
    },
    semantic::hir::{self, ColumnUsage, HirDocument, QueryBlockId, QueryId, SourceId},
};
use crate::{schema::Schema, sync::Arc, LimboError, Result};
use rustc_hash::FxHashSet as HashSet;

/// Planned form of one resolved query. The semantic document stays alive so
/// source and output identities remain resolvable without copying definitions.
pub(crate) struct HirQueryPlan {
    pub(crate) document: Arc<HirDocument>,
    pub(crate) query: QueryId,
    pub(crate) queries: Vec<HirPlannedQuery>,
}

/// Access plans and estimates for one query in the owned HIR document.
pub(crate) struct HirPlannedQuery {
    pub(crate) query: QueryId,
    pub(crate) blocks: Vec<HirQueryBlockPlan>,
    pub(crate) output_cardinality: f64,
    pub(crate) cost: Cost,
}

impl HirQueryPlan {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn build(
        document: Arc<HirDocument>,
        query: QueryId,
        schema: &Schema,
        params: &CostModelParams,
    ) -> Result<Self> {
        let mut queries = Vec::new();
        HirPlanContext::new(&document).plan_query_tree(
            query,
            schema,
            params,
            &mut queries,
            &mut HashSet::default(),
        )?;
        queries.sort_unstable_by_key(|plan| plan.query.index());
        Ok(Self {
            document,
            query,
            queries,
        })
    }

    fn planned_query(&self, query: QueryId) -> Option<&HirPlannedQuery> {
        self.queries.iter().find(|plan| plan.query == query)
    }
}

/// Resolved source and predicate input for one query block.
pub(crate) struct HirQueryBlockPlanInput {
    pub(crate) sources: Vec<HirPlanSource>,
    pub(crate) predicates: Vec<HirWhereTerm>,
}

/// Access choices for one resolved HIR query block.
pub(crate) struct HirQueryBlockPlan {
    pub(crate) block: QueryBlockId,
    pub(crate) loops: Vec<HirPlannedLoop>,
    pub(crate) predicates: Vec<HirWhereTerm>,
    pub(crate) output_cardinality: f64,
    pub(crate) cost: Cost,
}

/// One selected source loop in physical join order, still using HIR
/// expressions and document-local source identity.
pub(crate) struct HirPlannedLoop {
    pub(crate) source: SourceId,
    pub(crate) source_position: usize,
    pub(crate) access: HirSourceAccess,
}

/// Selected access for one resolved HIR source.
pub(crate) enum HirSourceAccess {
    BTree(HirBtreeOperation),
    Derived {
        query: QueryId,
    },
    Cte {
        cte: hir::CteId,
        query: QueryId,
        materialization: HirCteMaterialization,
    },
}

#[derive(Clone, Copy)]
struct HirQueryEstimate {
    output_cardinality: f64,
    cost: Cost,
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

    /// Plan derived-query dependencies before their owning query so source
    /// scans can use the child's output and work estimates.
    fn plan_query_tree(
        &self,
        query_id: QueryId,
        schema: &Schema,
        params: &CostModelParams,
        planned: &mut Vec<HirPlannedQuery>,
        active: &mut HashSet<QueryId>,
    ) -> Result<HirQueryEstimate> {
        if let Some(plan) = planned.iter().find(|plan| plan.query == query_id) {
            return Ok(HirQueryEstimate {
                output_cardinality: plan.output_cardinality,
                cost: plan.cost,
            });
        }
        if !active.insert(query_id) {
            return Err(LimboError::InternalError(format!(
                "derived HIR query dependency cycle at {query_id}"
            )));
        }

        let query = self
            .document
            .query(query_id)
            .expect("validated HIR query exists");
        let mut dependencies = Vec::new();
        for block in &query.blocks {
            let Some(from) = &block.from else {
                continue;
            };
            for source in
                core::iter::once(from.first).chain(from.joins.iter().map(|join| join.right))
            {
                match &self.definition(source).kind {
                    hir::SourceKind::Derived(dependency) => dependencies.push(*dependency),
                    hir::SourceKind::Cte(cte) => dependencies.push(self.cte_query(*cte)?),
                    _ => {}
                }
            }
        }
        dependencies.sort_unstable();
        dependencies.dedup();
        for dependency in dependencies {
            self.plan_query_tree(dependency, schema, params, planned, active)?;
        }

        let block_order_by = query
            .compounds
            .is_empty()
            .then_some(query.order_by.as_slice())
            .unwrap_or_default();
        let query_estimate = |query| {
            planned
                .iter()
                .find(|plan| plan.query == query)
                .map(|plan| HirQueryEstimate {
                    output_cardinality: plan.output_cardinality,
                    cost: plan.cost,
                })
        };
        let blocks = query
            .blocks
            .iter()
            .map(|block| {
                self.plan_query_block(block, block_order_by, 1.0, schema, params, &query_estimate)
            })
            .collect::<Result<Vec<_>>>()?;
        let estimate = query_estimate_from_blocks(query, &blocks, params);
        planned.push(HirPlannedQuery {
            query: query_id,
            blocks,
            output_cardinality: estimate.output_cardinality,
            cost: estimate.cost,
        });
        active.remove(&query_id);
        Ok(estimate)
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

    fn cte_query(&self, cte: hir::CteId) -> Result<QueryId> {
        let definition = self
            .document
            .cte(cte)
            .expect("validated HIR contains referenced CTE");
        match &definition.body {
            hir::CteBody::Query(query) => Ok(*query),
            hir::CteBody::Recursive(_) => Err(LimboError::InternalError(format!(
                "recursive CTE {cte} cannot be planned as a non-recursive HIR source"
            ))),
        }
    }

    fn cte_materialization(
        &self,
        owner: QueryId,
        cte: hir::CteId,
        query: QueryId,
    ) -> HirCteMaterialization {
        let definition = self
            .document
            .cte(cte)
            .expect("validated HIR contains referenced CTE");
        if matches!(
            definition.materialized,
            turso_parser::ast::Materialized::Yes
        ) {
            return HirCteMaterialization::Explicit;
        }

        if self.cte_reference_count(owner, cte) > 1 && !self.query_tree_has_outer_dependency(query)
        {
            HirCteMaterialization::Shared
        } else {
            HirCteMaterialization::PerReference
        }
    }

    /// Count references in one query plan. A CTE body is a separate plan tree:
    /// when reached through a CTE source, its nested references are classified
    /// while that body is planned instead of being multiplied by its callers.
    fn cte_reference_count(&self, root: QueryId, target: hir::CteId) -> usize {
        self.document
            .queries
            .iter()
            .filter(|query| self.query_belongs_to_tree(query.id, root, true))
            .flat_map(|query| &query.blocks)
            .filter_map(|block| block.from.as_ref())
            .map(|from| self.count_cte_references_in_from(from, target))
            .sum()
    }

    fn count_cte_references_in_from(&self, from: &hir::From, target: hir::CteId) -> usize {
        core::iter::once(from.first)
            .chain(from.joins.iter().map(|join| join.right))
            .map(|source| self.count_cte_references_in_source(source, target))
            .sum()
    }

    fn count_cte_references_in_source(&self, source: SourceId, target: hir::CteId) -> usize {
        match &self.definition(source).kind {
            hir::SourceKind::Cte(cte) => usize::from(*cte == target),
            hir::SourceKind::FromGroup(group) => {
                self.count_cte_references_in_from(&group.from, target)
            }
            _ => 0,
        }
    }

    fn query_tree_has_outer_dependency(&self, root: QueryId) -> bool {
        let queries = self
            .document
            .queries
            .iter()
            .filter(|query| self.query_belongs_to_tree(query.id, root, false))
            .map(|query| query.id)
            .collect::<HashSet<_>>();

        self.document.queries.iter().any(|query| {
            queries.contains(&query.id)
                && query.captures.iter().any(|source| {
                    let source = self.definition(*source);
                    match source.owner {
                        hir::SourceOwner::QueryBlock(block) => !queries.contains(&block.query),
                        hir::SourceOwner::Cte(cte) => !self.cte_belongs_to_queries(cte, &queries),
                        hir::SourceOwner::Root => true,
                    }
                })
        })
    }

    fn query_belongs_to_tree(
        &self,
        mut query: QueryId,
        root: QueryId,
        stop_at_cte_body: bool,
    ) -> bool {
        loop {
            if query == root {
                return true;
            }
            if stop_at_cte_body && self.is_cte_body_query(query) {
                return false;
            }
            let Some(parent) = self
                .document
                .query(query)
                .expect("validated HIR query exists")
                .parent
            else {
                return false;
            };
            query = parent;
        }
    }

    fn is_cte_body_query(&self, query: QueryId) -> bool {
        self.document.ctes.iter().any(|cte| match &cte.body {
            hir::CteBody::Query(body) => *body == query,
            hir::CteBody::Recursive(recursive) => {
                recursive.seed == query || recursive.arms.iter().any(|arm| arm.query == query)
            }
        })
    }

    fn cte_belongs_to_queries(&self, cte: hir::CteId, queries: &HashSet<QueryId>) -> bool {
        let definition = self
            .document
            .cte(cte)
            .expect("validated HIR contains captured CTE source");
        match &definition.body {
            hir::CteBody::Query(query) => queries.contains(query),
            hir::CteBody::Recursive(recursive) => {
                queries.contains(&recursive.seed)
                    || recursive
                        .arms
                        .iter()
                        .any(|arm| queries.contains(&arm.query))
            }
        }
    }

    fn plan_source(
        &self,
        source: SourceId,
        join_info: Option<HirJoinInfo>,
        usage: &[ColumnUsage],
    ) -> Result<HirPlanSource> {
        match &self.definition(source).kind {
            hir::SourceKind::Table(_) => self
                .source(source, join_info, usage)
                .map(HirPlanSource::BTree),
            hir::SourceKind::Derived(query) => Ok(HirPlanSource::Derived {
                source,
                query: *query,
                join_info,
            }),
            hir::SourceKind::Cte(cte) => {
                let query = self.cte_query(*cte)?;
                let owner = match self.definition(source).owner {
                    hir::SourceOwner::QueryBlock(block) => block.query,
                    owner => {
                        return Err(LimboError::InternalError(format!(
                            "CTE HIR source {source} has non-query owner {owner:?}"
                        )));
                    }
                };
                Ok(HirPlanSource::Cte {
                    source,
                    cte: *cte,
                    query,
                    materialization: self.cte_materialization(owner, *cte, query),
                    join_info,
                })
            }
            _ => Err(LimboError::InternalError(format!(
                "source {source} cannot be planned as a query source"
            ))),
        }
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
            input
                .sources
                .push(self.plan_source(from.first, None, usage)?);
            for join in &from.joins {
                input.sources.push(self.plan_source(
                    join.right,
                    Some(HirJoinInfo { kind: join.kind }),
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
        base_rows: &[RowCountEstimate],
        schema: &Schema,
        params: &CostModelParams,
        query_rows: &dyn Fn(QueryId) -> Option<f64>,
    ) -> Result<Vec<HirTableConstraints>> {
        assert_eq!(
            input.sources.len(),
            base_rows.len(),
            "every HIR plan source must have a base-row estimate"
        );
        input
            .sources
            .iter()
            .zip(base_rows)
            .map(|(source, row_count)| {
                let source = match source {
                    HirPlanSource::BTree(source) => HirConstraintSource::BTree(source),
                    HirPlanSource::Derived { source, .. } => HirConstraintSource::Derived {
                        source: *source,
                        row_count: *row_count,
                    },
                    HirPlanSource::Cte { source, .. } => HirConstraintSource::Cte {
                        source: *source,
                        row_count: *row_count,
                    },
                };
                hir_constraints_for_source(
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

    /// Plan one resolved query block without exposing mutable planner input.
    #[allow(clippy::too_many_arguments)]
    fn plan_query_block(
        &self,
        block: &hir::QueryBlock,
        order_by: &[hir::OrderTerm],
        initial_cardinality: f64,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirQueryBlockPlan> {
        let query = self
            .document
            .query(block.id.query)
            .expect("validated HIR query block has owning query");
        let usage = query.direct_column_usage(|source| self.document.source(source));
        let Some(from) = &block.from else {
            let input = self.query_block_input(block, order_by, &usage)?;
            let output_cardinality = match &block.body {
                hir::QueryBlockBody::Values { rows } => initial_cardinality * rows.len() as f64,
                hir::QueryBlockBody::Select { .. } => initial_cardinality,
            };
            return Ok(HirQueryBlockPlan {
                block: block.id,
                loops: Vec::new(),
                predicates: input.predicates,
                output_cardinality,
                cost: Cost(0.0),
            });
        };

        let input = self.query_block_input(block, order_by, &usage)?;
        let order_target = self.order_target(
            order_by,
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
        );
        self.plan_source_access(
            block.id,
            from,
            input,
            order_target.as_ref(),
            initial_cardinality,
            schema,
            params,
            query_estimate,
        )?
        .ok_or_else(|| {
            LimboError::InternalError(
                "query block with FROM produced no B-tree access plan".to_string(),
            )
        })
    }

    /// Choose source access methods and a complete join order directly from
    /// resolved HIR. This keeps the planning boundary free of parser-era table
    /// identities, synthetic subquery tables, and bound AST expressions.
    #[allow(clippy::too_many_arguments)]
    fn plan_source_access(
        &self,
        block: QueryBlockId,
        from: &hir::From,
        mut input: HirQueryBlockPlanInput,
        order_target: Option<&HirOrderTarget<'_>>,
        initial_cardinality: f64,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<Option<HirQueryBlockPlan>> {
        let query_rows = |query| query_estimate(query).map(|estimate| estimate.output_cardinality);
        let mut base_rows = Vec::with_capacity(input.sources.len());
        let mut source_costs = Vec::with_capacity(input.sources.len());
        for source in &input.sources {
            match source {
                HirPlanSource::BTree(source) => {
                    base_rows.push(base_row_estimate(schema, &source.table, params));
                    source_costs.push(Cost(0.0));
                }
                HirPlanSource::Derived { source, query, .. } => {
                    let child = query_estimate(*query).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "derived HIR source {source} references unplanned query {query}"
                        ))
                    })?;
                    base_rows.push(RowCountEstimate::AnalyzeStats(
                        child.output_cardinality.max(1.0),
                    ));
                    source_costs.push(child.cost);
                }
                HirPlanSource::Cte {
                    source, cte, query, ..
                } => {
                    let child = query_estimate(*query).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "CTE HIR source {source} for {cte} references unplanned query {query}"
                        ))
                    })?;
                    base_rows.push(RowCountEstimate::AnalyzeStats(
                        child.output_cardinality.max(1.0),
                    ));
                    source_costs.push(child.cost);
                }
            }
        }
        let constraints =
            self.constraints(from, &input, &base_rows, schema, params, &query_rows)?;
        let mut access_methods = Vec::new();
        let result = compute_hir_greedy_join_order(
            self.document,
            from,
            &input.sources,
            &constraints,
            &input.predicates,
            &base_rows,
            &source_costs,
            order_target,
            initial_cardinality,
            &mut access_methods,
            schema,
            &schema.analyze_stats,
            params,
        )?;

        let Some(result) = result else {
            return Ok(None);
        };
        let mut prior_sources = crate::translate::planner::TableMask::default();
        let mut loops = Vec::with_capacity(input.sources.len());
        for (source_position, access_method_position) in result.best_plan.data.iter().copied() {
            let access = match &input.sources[source_position] {
                HirPlanSource::BTree(source) => {
                    HirSourceAccess::BTree(apply_hir_selected_btree_access(
                        source,
                        &constraints[source_position],
                        &mut input.predicates,
                        &mut access_methods[access_method_position],
                        &prior_sources,
                        source_position,
                    )?)
                }
                HirPlanSource::Derived { query, .. } => {
                    if !matches!(
                        access_methods[access_method_position].params,
                        super::optimizer::access_method::AccessMethodParams::Subquery { .. }
                    ) {
                        return Err(LimboError::InternalError(
                            "derived HIR source selected a non-subquery access method".to_string(),
                        ));
                    }
                    HirSourceAccess::Derived { query: *query }
                }
                HirPlanSource::Cte {
                    cte,
                    query,
                    materialization,
                    ..
                } => {
                    if !matches!(
                        access_methods[access_method_position].params,
                        super::optimizer::access_method::AccessMethodParams::Subquery { .. }
                    ) {
                        return Err(LimboError::InternalError(
                            "CTE HIR source selected a non-subquery access method".to_string(),
                        ));
                    }
                    HirSourceAccess::Cte {
                        cte: *cte,
                        query: *query,
                        materialization: *materialization,
                    }
                }
            };
            loops.push(HirPlannedLoop {
                source: input.sources[source_position].source(),
                source_position,
                access,
            });
            prior_sources.set(source_position)?;
        }

        Ok(Some(HirQueryBlockPlan {
            block,
            loops,
            predicates: input.predicates,
            output_cardinality: result.best_plan.output_cardinality,
            cost: result.best_plan.cost,
        }))
    }

    /// Build an ordering requirement from resolved HIR terms without reopening
    /// name resolution or copying expressions out of the document.
    pub(crate) fn order_target<'term>(
        &self,
        terms: &'term [hir::OrderTerm],
        purpose: OrderTargetPurpose,
    ) -> Option<HirOrderTarget<'term>>
    where
        'a: 'term,
    {
        let columns = terms
            .iter()
            .map(|term| self.order_column(term))
            .collect::<Option<Vec<_>>>()?;
        (!columns.is_empty()).then_some(OrderTarget { columns, purpose })
    }

    fn order_column<'term>(
        &self,
        term: &'term hir::OrderTerm,
    ) -> Option<ColumnOrder<SourceId, &'term hir::Expr>>
    where
        'a: 'term,
    {
        let mut expression = &term.expr;
        while let hir::Expr::Output(output) = expression {
            expression = &self.document.output(*output)?.expr;
        }

        let (source, target) = match expression {
            hir::Expr::Column(column) => (column.source, ColumnTarget::Column(column.column)),
            hir::Expr::RowId(source) => (*source, ColumnTarget::RowId),
            expression => {
                let (source, _) = expression.single_source_column_usage()?;
                (source, ColumnTarget::Expr(expression))
            }
        };

        Some(ColumnOrder {
            source,
            target,
            order: term.order,
            collation: term
                .collation
                .as_ref()
                .map(|collation| *collation.value())
                .unwrap_or_default(),
            nulls_order: term.nulls,
        })
    }

    fn register_expression_index_usage(
        &self,
        sources: &mut [HirPlanSource],
        expression: &hir::Expr,
    ) -> Result<()> {
        let Some((source, columns)) = expression.single_source_column_usage() else {
            return Ok(());
        };
        let Some(planned) = sources
            .iter_mut()
            .find(|planned| planned.source() == source)
            .and_then(HirPlanSource::btree_mut)
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

fn query_estimate_from_blocks(
    query: &hir::Query,
    blocks: &[HirQueryBlockPlan],
    params: &CostModelParams,
) -> HirQueryEstimate {
    let fallback = *RowCountEstimate::hardcoded_fallback(params);
    let block_rows = |block| {
        blocks
            .iter()
            .find(|plan| plan.block == block)
            .map_or(fallback, |plan| plan.output_cardinality)
    };
    let mut output_cardinality = block_rows(query.first);
    for arm in &query.compounds {
        let rhs = block_rows(arm.block);
        output_cardinality = match arm.operator {
            turso_parser::ast::CompoundOperator::Union
            | turso_parser::ast::CompoundOperator::UnionAll => output_cardinality + rhs,
            turso_parser::ast::CompoundOperator::Except => output_cardinality,
            turso_parser::ast::CompoundOperator::Intersect => output_cardinality.min(rhs),
        };
    }

    HirQueryEstimate {
        output_cardinality,
        cost: blocks
            .iter()
            .fold(Cost(0.0), |cost, block| cost + block.cost),
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
        op: (),
        table: table.value().clone(),
        identifier: (),
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
        translate::collate::CollationSeq,
        translate::{
            optimizer::cost::RowCountEstimate,
            semantic::hir::{
                CatalogObject, CatalogObjectId, CatalogSnapshot, ColumnReadExpression,
                ComparisonComponent, ComparisonSemantics, CompoundArm, Cte, CteBody, CteColumn,
                CteId, DatabaseId, DatabaseSnapshot, IndexCoverage, Join, JoinConstraint, JoinKind,
                OrderTerm, Output, OutputId, Query, QueryBlock, QueryBlockBody, QueryBlockId,
                QueryId, QueryRoot, SourceColumn, SourceKind, SourceOwner, TypeFact, UsingColumn,
            },
        },
        vdbe::affinity::Affinity,
    };
    use turso_parser::ast::{CompoundOperator, Materialized, NullsOrder, Operator, SortOrder};

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

    fn add_query(document: &mut hir::HirDocument, block: &QueryBlock) {
        let query = block.id.query;
        document.root = hir::HirRoot::Query(QueryRoot { query });
        document.queries.push(Query {
            id: query,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![block.clone()],
            first: block.id,
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: block.outputs.iter().map(|output| output.id).collect(),
        });
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
    fn hir_order_target_resolves_outputs_and_borrows_expressions() {
        let source = SourceId::new(0);
        let query = QueryId::new(0);
        let block_id = QueryBlockId::new(query, 0);
        let output_id = OutputId::query(block_id, 0);
        let mut block = QueryBlock::new(
            block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.outputs.push(Output {
            id: output_id,
            name: "first".to_string(),
            expr: hir::Expr::column(source, 0),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: true,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        });
        let mut document = document(vec![source_with_id(0, "items")]);
        document.root = hir::HirRoot::Query(QueryRoot { query });
        document.queries.push(Query {
            id: query,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![block],
            first: block_id,
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: vec![output_id],
        });

        let computed = binary(
            hir::Expr::column(source, 0),
            Operator::Add,
            hir::Expr::column(source, 1),
        );
        let collation = CatalogObject::new(
            CatalogObjectId::new(20),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(CollationSeq::NoCase),
        );
        let terms = [
            OrderTerm {
                expr: hir::Expr::output(output_id),
                order: SortOrder::Desc,
                nulls: Some(NullsOrder::First),
                type_fact: TypeFact::known(Type::Integer),
                collation: None,
            },
            OrderTerm {
                expr: computed,
                order: SortOrder::Asc,
                nulls: None,
                type_fact: TypeFact::known(Type::Integer),
                collation: Some(collation),
            },
        ];
        let target = HirPlanContext::new(&document)
            .order_target(
                &terms,
                OrderTargetPurpose::EliminatesSort(
                    crate::translate::optimizer::order::EliminatesSortBy::Order,
                ),
            )
            .expect("resolved terms form one-source order target");

        assert!(matches!(target.columns[0].target, ColumnTarget::Column(0)));
        assert_eq!(target.columns[0].source, source);
        assert_eq!(target.columns[0].order, SortOrder::Desc);
        assert_eq!(target.columns[0].nulls_order, Some(NullsOrder::First));
        let ColumnTarget::Expr(expression) = target.columns[1].target else {
            panic!("computed HIR order term remains an expression");
        };
        assert!(std::ptr::eq(expression, &terms[1].expr));
        assert_eq!(target.columns[1].collation, CollationSeq::NoCase);
    }

    #[test]
    fn hir_source_reuses_identity_and_table_metadata() {
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
                }),
                &[],
            )
            .expect("table source converts");

        assert_eq!(joined.internal_id, source.id);
        assert_eq!(
            joined.join_info.as_ref().unwrap().kind,
            hir::JoinKind::Inner
        );
        assert_eq!(joined.database_id, 0);
        let (Table::BTree(expected), Table::BTree(actual)) = (resolved.value(), &joined.table)
        else {
            panic!("resolved and planned tables are btree tables");
        };
        assert!(Arc::ptr_eq(expected, actual));
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
        let HirPlanSource::BTree(planned) = &mut planned else {
            panic!("table source carries B-tree planning metadata");
        };

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
                .map(HirPlanSource::source)
                .collect::<Vec<_>>(),
            vec![left, right]
        );
        assert!(input.sources[0].join_info().is_none());
        let join = input.sources[1].join_info().unwrap();
        assert_eq!(join.kind, JoinKind::Right);
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
        let mut document = document(vec![
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

        let usage = [
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: left,
                    column: 0,
                },
                count: 1,
            },
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: right,
                    column: 0,
                },
                count: 1,
            },
        ];

        add_query(&mut document, &block);

        let context = HirPlanContext::new(&document);
        let input = context
            .query_block_input(&block, &[], &usage)
            .expect("query block input converts");

        assert_eq!(input.sources[1].join_info().unwrap().kind, JoinKind::Left);
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

        let params = CostModelParams::default();
        let mut schema = Schema::default();
        schema.analyze_stats.table_stats_mut("left_items").row_count = Some(1_000);
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(1);
        let base_rows = input
            .sources
            .iter()
            .map(|source| {
                base_row_estimate(
                    &schema,
                    &source.btree().expect("test source is a table").table,
                    &params,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            base_rows,
            [
                RowCountEstimate::AnalyzeStats(1_000.0),
                RowCountEstimate::AnalyzeStats(1.0),
            ]
        );
        let query_id = block.id.query;
        let document = Arc::new(document);
        let query_plan = HirQueryPlan::build(document.clone(), query_id, &schema, &params)
            .expect("HIR query planning succeeds");
        assert!(Arc::ptr_eq(&query_plan.document, &document));
        assert_eq!(query_plan.query, query_id);
        let planned_query = query_plan
            .planned_query(query_id)
            .expect("root query is planned");
        let [plan] = planned_query.blocks.as_slice() else {
            panic!("ordinary SELECT has one planned block");
        };
        assert_eq!(plan.block, block.id);
        assert!(plan.output_cardinality > 0.0);
        assert!(plan.cost.0 >= 0.0);
        let [left_loop, right_loop] = plan.loops.as_slice() else {
            panic!("two sources produce two planned loops");
        };
        assert_eq!(left_loop.source, left);
        assert_eq!(left_loop.source_position, 0);
        assert_eq!(right_loop.source, right);
        assert_eq!(right_loop.source_position, 1);
        let HirSourceAccess::BTree(HirBtreeOperation::Seek {
            index: Some(index),
            seek_def,
        }) = &right_loop.access
        else {
            panic!("right source uses its selected automatic-index seek");
        };
        assert!(index.ephemeral);
        assert_eq!(
            index
                .columns
                .iter()
                .map(|column| column.pos_in_table)
                .collect::<Vec<_>>(),
            [0]
        );
        let Some((operator, expression, affinity)) = &seek_def.prefix[0].eq else {
            panic!("join equality becomes the seek prefix");
        };
        assert_eq!(*operator, Operator::Equals);
        assert_eq!(*affinity, Affinity::Integer);
        assert!(matches!(
            expression,
            hir::Expr::Column(column) if *column == hir::ColumnRef { source: left, column: 0 }
        ));
        assert!(plan.predicates[0].consumed);
    }

    #[test]
    fn source_less_query_block_keeps_predicates_without_access_loops() {
        let mut document = document(vec![source()]);
        let block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                    "1".into(),
                ))),
                grouping: None,
            },
        );
        add_query(&mut document, &block);
        let plan = HirPlanContext::new(&document)
            .plan_query_block(
                &block,
                &[],
                7.0,
                &Schema::default(),
                &CostModelParams::default(),
                &|_| None,
            )
            .expect("source-less query block plans");

        assert!(plan.loops.is_empty());
        assert_eq!(plan.predicates.len(), 1);
        assert!(!plan.predicates[0].consumed);
        assert_eq!(plan.output_cardinality, 7.0);
        assert_eq!(plan.cost, Cost(0.0));
    }

    #[test]
    fn owned_hir_query_plan_plans_values_block() {
        let mut document = document(vec![source()]);
        let block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Values {
                rows: vec![vec![hir::Expr::Literal(
                    turso_parser::ast::Literal::Numeric("1".into()),
                )]],
            },
        );
        add_query(&mut document, &block);
        let document = Arc::new(document);

        let plan = HirQueryPlan::build(
            document.clone(),
            block.id.query,
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("VALUES query plans from HIR");

        assert!(Arc::ptr_eq(&plan.document, &document));
        assert_eq!(plan.query, block.id.query);
        let query = plan
            .planned_query(block.id.query)
            .expect("root query is planned");
        assert_eq!(query.blocks.len(), 1);
        assert_eq!(query.blocks[0].block, block.id);
        assert!(query.blocks[0].loops.is_empty());
        assert!(query.blocks[0].predicates.is_empty());
        assert_eq!(query.blocks[0].output_cardinality, 1.0);
        assert_eq!(query.blocks[0].cost, Cost(0.0));
    }

    #[test]
    fn owned_hir_query_plan_plans_compound_arms_without_arm_ordering() {
        let query_id = QueryId::new(0);
        let first_id = QueryBlockId::new(query_id, 0);
        let second_id = QueryBlockId::new(query_id, 1);
        let value = || QueryBlockBody::Values {
            rows: vec![vec![hir::Expr::Literal(
                turso_parser::ast::Literal::Numeric("1".into()),
            )]],
        };
        let first = QueryBlock::new(first_id, value());
        let second = QueryBlock::new(second_id, value());
        let mut document = document(vec![source()]);
        document.root = hir::HirRoot::Query(QueryRoot { query: query_id });
        document.queries.push(Query {
            id: query_id,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![first, second],
            first: first_id,
            compounds: vec![CompoundArm {
                operator: CompoundOperator::UnionAll,
                block: second_id,
            }],
            order_by: vec![OrderTerm {
                expr: hir::Expr::Literal(turso_parser::ast::Literal::Numeric("1".into())),
                order: SortOrder::Desc,
                nulls: None,
                type_fact: TypeFact::known(Type::Integer),
                collation: None,
            }],
            limit: None,
            output: Vec::new(),
        });
        let document = Arc::new(document);

        let plan = HirQueryPlan::build(
            document.clone(),
            query_id,
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("compound query arms plan from HIR");

        let planned_query = plan.planned_query(query_id).expect("root query is planned");
        assert_eq!(
            planned_query
                .blocks
                .iter()
                .map(|block| block.block)
                .collect::<Vec<_>>(),
            [first_id, second_id]
        );
        assert_eq!(planned_query.output_cardinality, 2.0);
        let query = plan.document.query(query_id).expect("query remains owned");
        assert_eq!(query.compounds.len(), 1);
        assert_eq!(query.order_by.len(), 1);
    }

    #[test]
    fn owned_hir_query_plan_plans_derived_query_before_its_scan() {
        let root_query = QueryId::new(0);
        let child_query = QueryId::new(1);
        let root_block_id = QueryBlockId::new(root_query, 0);
        let child_block_id = QueryBlockId::new(child_query, 0);
        let source_id = SourceId::new(0);

        let mut root_block = QueryBlock::new(
            root_block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::column(source_id, 0)),
                    operator: Operator::Equals,
                    rhs: Box::new(hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                        "1".into(),
                    ))),
                    array_concat: false,
                    custom: None,
                    comparison: Some(ComparisonSemantics {
                        components: vec![ComparisonComponent {
                            affinity: Affinity::Integer,
                            collation: None,
                            array: false,
                        }],
                    }),
                }),
                grouping: None,
            },
        );
        root_block.from = Some(hir::From {
            first: source_id,
            joins: Vec::new(),
        });
        let mut child_block = QueryBlock::new(
            child_block_id,
            QueryBlockBody::Values {
                rows: vec![
                    vec![hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                        "1".into(),
                    ))],
                    vec![hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                        "2".into(),
                    ))],
                ],
            },
        );
        child_block.outputs.push(Output {
            id: OutputId::query(child_block_id, 0),
            name: "column1".to_string(),
            expr: hir::Expr::Literal(turso_parser::ast::Literal::Numeric("1".into())),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: false,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        });
        let query = |id, parent, block: QueryBlock| {
            let output = block.outputs.iter().map(|output| output.id).collect();
            Query {
                id,
                parent,
                captures: Vec::new(),
                reachable_ctes: Vec::new(),
                first: block.id,
                blocks: vec![block],
                compounds: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                output,
            }
        };
        let mut derived_source = source();
        derived_source.id = source_id;
        derived_source.owner = SourceOwner::QueryBlock(root_block_id);
        derived_source.database = None;
        derived_source.kind = SourceKind::Derived(child_query);
        derived_source.columns.truncate(1);
        derived_source.generated_expressions.truncate(1);
        derived_source.default_expressions.truncate(1);
        derived_source.column_type_programs.truncate(1);
        let mut document = document(vec![derived_source]);
        document.root = hir::HirRoot::Query(QueryRoot { query: root_query });
        document.queries = vec![
            query(root_query, None, root_block),
            query(child_query, Some(root_query), child_block),
        ];
        document.validate().expect("derived query HIR is valid");
        let plan = HirQueryPlan::build(
            Arc::new(document),
            root_query,
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("derived query plans from HIR");

        let child = plan
            .planned_query(child_query)
            .expect("child query is planned");
        assert_eq!(child.output_cardinality, 2.0);
        let root = plan
            .planned_query(root_query)
            .expect("root query is planned");
        assert_eq!(root.output_cardinality, 0.2);
        let [source_loop] = root.blocks[0].loops.as_slice() else {
            panic!("derived source produces one scan loop");
        };
        assert_eq!(source_loop.source, source_id);
        assert_eq!(source_loop.source_position, 0);
        assert!(matches!(
            source_loop.access,
            HirSourceAccess::Derived { query } if query == child_query
        ));
        assert!(root.cost.0 > child.cost.0);

        let cte_id = CteId::new(0);
        let mut cte_document = plan.document.as_ref().clone();
        cte_document.sources[source_id.index()].kind = SourceKind::Cte(cte_id);
        cte_document.queries[root_query.index()].reachable_ctes = vec![cte_id];
        let source_column = &cte_document.sources[source_id.index()].columns[0];
        cte_document.ctes.push(Cte {
            id: cte_id,
            name: "numbers".to_string(),
            columns: vec![CteColumn {
                name: source_column.name.clone(),
                type_fact: source_column.type_fact.clone(),
                affinity: source_column.affinity,
                has_affinity: source_column.has_affinity,
                collation: source_column.collation.clone(),
            }],
            materialized: Materialized::Yes,
            body: CteBody::Query(child_query),
        });
        cte_document
            .validate()
            .expect("non-recursive CTE query HIR is valid");
        let materialization =
            HirPlanContext::new(&cte_document).cte_materialization(root_query, cte_id, child_query);
        assert_eq!(materialization, HirCteMaterialization::Explicit);
        let cte_plan = HirQueryPlan::build(
            Arc::new(cte_document.clone()),
            root_query,
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("non-recursive CTE source plans from HIR");
        let cte_root = cte_plan
            .planned_query(root_query)
            .expect("CTE root query is planned");
        assert_eq!(cte_root.output_cardinality, 0.2);
        let [cte_loop] = cte_root.blocks[0].loops.as_slice() else {
            panic!("CTE source produces one scan loop");
        };
        assert!(matches!(
            cte_loop.access,
            HirSourceAccess::Cte {
                cte,
                query,
                materialization: HirCteMaterialization::Explicit,
            } if cte == cte_id && query == child_query
        ));

        cte_document.ctes[cte_id.index()].materialized = Materialized::Any;
        let materialization =
            HirPlanContext::new(&cte_document).cte_materialization(root_query, cte_id, child_query);
        assert_eq!(materialization, HirCteMaterialization::PerReference);

        let second_cte_source = SourceId::new(cte_document.sources.len());
        let mut second_reference = cte_document.sources[source_id.index()].clone();
        second_reference.id = second_cte_source;
        cte_document.sources.push(second_reference);
        cte_document.queries[root_query.index()].blocks[0]
            .from
            .as_mut()
            .expect("root query has a CTE source")
            .joins
            .push(Join {
                right: second_cte_source,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            });
        cte_document.ctes[cte_id.index()].materialized = Materialized::No;
        cte_document
            .validate()
            .expect("two references to a NOT MATERIALIZED CTE are valid");

        let mut correlated_document = cte_document.clone();
        let outer_source = SourceId::new(correlated_document.sources.len());
        let mut outer_definition = source_with_id(outer_source.index(), "outer_items");
        outer_definition.owner = SourceOwner::QueryBlock(root_block_id);
        correlated_document.sources.push(outer_definition);
        correlated_document.databases.push(DatabaseSnapshot {
            database: DatabaseId::new(0),
            schema_version: 0,
        });
        correlated_document.queries[root_query.index()].blocks[0]
            .from
            .as_mut()
            .expect("root query has CTE references")
            .joins
            .push(Join {
                right: outer_source,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            });
        correlated_document.queries[child_query.index()].blocks[0].outputs[0].expr =
            hir::Expr::column(outer_source, 0);
        correlated_document.queries[child_query.index()].captures = vec![outer_source];
        correlated_document
            .validate()
            .expect("correlated CTE body HIR is valid");
        assert_eq!(
            HirPlanContext::new(&correlated_document).cte_materialization(
                root_query,
                cte_id,
                child_query,
            ),
            HirCteMaterialization::PerReference,
        );

        let shared_plan = HirQueryPlan::build(
            Arc::new(cte_document),
            root_query,
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("repeated CTE references plan from HIR");
        assert!(shared_plan
            .planned_query(root_query)
            .expect("shared CTE root query is planned")
            .blocks[0]
            .loops
            .iter()
            .all(|source_loop| matches!(
                source_loop.access,
                HirSourceAccess::Cte {
                    materialization: HirCteMaterialization::Shared,
                    ..
                }
            )));

        let table_source_id = SourceId::new(1);
        let mut table_source = source_with_id(1, "items");
        table_source.owner = SourceOwner::QueryBlock(root_block_id);
        let mut mixed_document = plan.document.as_ref().clone();
        mixed_document.databases.push(DatabaseSnapshot {
            database: DatabaseId::new(0),
            schema_version: 0,
        });
        mixed_document.sources.push(table_source);
        let root_block = &mut mixed_document.queries[root_query.index()].blocks[0];
        root_block.from = Some(hir::From {
            first: table_source_id,
            joins: vec![Join {
                right: source_id,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::column(table_source_id, 0)),
                    operator: Operator::Equals,
                    rhs: Box::new(hir::Expr::column(source_id, 0)),
                    array_concat: false,
                    custom: None,
                    comparison: Some(ComparisonSemantics {
                        components: vec![ComparisonComponent {
                            affinity: Affinity::Integer,
                            collation: None,
                            array: false,
                        }],
                    }),
                }),
            }],
        });
        mixed_document
            .validate()
            .expect("mixed table and derived query HIR is valid");
        let mixed_plan = HirQueryPlan::build(
            Arc::new(mixed_document),
            root_query,
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("table and derived source join plans from HIR");
        let mixed_root = mixed_plan
            .planned_query(root_query)
            .expect("mixed root query is planned");
        assert_eq!(mixed_root.blocks[0].loops.len(), 2);
        assert!(mixed_root.blocks[0].loops.iter().any(|source_loop| {
            source_loop.source == table_source_id
                && matches!(source_loop.access, HirSourceAccess::BTree(_))
        }));
        assert!(mixed_root.blocks[0].loops.iter().any(|source_loop| {
            source_loop.source == source_id
                && matches!(
                    source_loop.access,
                    HirSourceAccess::Derived { query } if query == child_query
                )
        }));
    }
}
