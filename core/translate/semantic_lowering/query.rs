//! Runtime destination preparation for resolved HIR queries.

use turso_parser::ast::{Literal, SortOrder};

use crate::{
    schema::{BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, Table, Type},
    sync::Arc,
    translate::{
        eqp::EqpDetail,
        expr::ConditionMetadata,
        main_loop::{
            emit_autoindex, emit_in_seek_advance, emit_in_seek_start, open_in_seek_values_cursor,
            AutoIndexBuild, InSeekLoop, LeftJoinMetadata, SeekEmitter, SeekExpressionLowering,
        },
        optimizer::{HirBtreeOperation, HirInSeekSource, HirVirtualTableOperation},
        plan::{
            self, EphemeralRowidMode, HirCteMaterialization, HirSeekDef, IterationDirection,
            QueryDestination,
        },
        result_row::emit_columns_to_destination,
        semantic::hir::{self, HirDocument, QueryId, SourceId, SubqueryExpr},
        semantic_to_plan::{
            BtreeTableLookup, HirPlan, HirPlannedLoop, HirQueryBlockPlan, HirSourceAccess,
        },
    },
    util::parse_numeric_literal,
    vdbe::{
        affinity::Affinity,
        builder::{
            CursorType, MaterializedCteInfo, ProgramBuilder, SourceBinding, SubqueryBinding,
        },
        insn::Insn,
        BranchOffset, CursorID,
    },
    LimboError, Numeric, Result, Value,
};

/// Physical destination and execution facts for one expression subquery.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct PreparedSubquery {
    pub(crate) query: QueryId,
    pub(crate) destination: QueryDestination,
    pub(crate) correlated: bool,
}

struct DestinationBinding {
    destination: QueryDestination,
    binding: SubqueryBinding,
}

#[derive(Clone, Copy)]
struct QueryLimitRegisters {
    limit: usize,
    offset: Option<usize>,
}

fn open_hir_in_seek_source_cursor(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    index: Option<&Arc<Index>>,
    source: &HirInSeekSource,
) -> Result<CursorID> {
    match source {
        HirInSeekSource::Values { values, affinity } => open_in_seek_values_cursor(
            program,
            index,
            values,
            *affinity,
            |program, expression, target| {
                super::expr::translate_expr_no_constant_opt(program, document, expression, target)
                    .map(|_| ())
            },
        ),
        HirInSeekSource::Query { query } => {
            let Some(SubqueryBinding::InIndex { cursor }) = program.subquery_binding(*query) else {
                return Err(LimboError::InternalError(format!(
                    "HIR IN seek query {query} has no index runtime binding"
                )));
            };
            Ok(cursor)
        }
    }
}

struct HirSeekExpressionLowering<'a> {
    document: &'a HirDocument,
    source: &'a hir::Source,
}

impl SeekExpressionLowering<hir::Expr> for HirSeekExpressionLowering<'_> {
    fn emit_expression(
        &mut self,
        program: &mut ProgramBuilder,
        expression: &hir::Expr,
        target: usize,
    ) -> Result<()> {
        super::expr::translate_expr_no_constant_opt(program, self.document, expression, target)?;
        Ok(())
    }

    fn is_nonnull(&self, expression: &hir::Expr) -> bool {
        matches!(
            expression,
            hir::Expr::Literal(
                Literal::Numeric(_)
                    | Literal::String(_)
                    | Literal::Blob(_)
                    | Literal::True
                    | Literal::False
            )
        )
    }

    fn is_null_matching(
        &self,
        operator: turso_parser::ast::Operator,
        expression: &hir::Expr,
    ) -> bool {
        operator == turso_parser::ast::Operator::Is && !self.is_nonnull(expression)
    }

    fn affinity(&self, affinity: Affinity, _expression: &hir::Expr) -> Affinity {
        // HIR constraint planning already removes affinity work that cannot
        // change the resolved key expression.
        affinity
    }

    fn encode_index_keys(
        &mut self,
        program: &mut ProgramBuilder,
        index: &Arc<Index>,
        start_reg: usize,
        num_keys: usize,
        index_column_offset: usize,
    ) -> Result<()> {
        for position in 0..num_keys {
            let Some(index_column) = index.columns.get(index_column_offset + position) else {
                break;
            };
            let Some(programs) = self
                .source
                .column_type_programs
                .get(index_column.pos_in_table)
                .and_then(Option::as_ref)
            else {
                continue;
            };
            if programs.encode.is_empty() {
                continue;
            }
            let register = start_reg + position;
            let encoded = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg: register,
                target_pc: encoded,
            });
            super::expr::translate_schema_calls_in_place(
                program,
                self.document,
                &programs.encode,
                register,
            )?;
            program.preassign_label_to_next_insn(encoded);
        }
        Ok(())
    }

    fn rowid_affinity(&self) -> Affinity {
        self.source
            .columns
            .iter()
            .find(|column| column.rowid_alias)
            .map_or(Affinity::Numeric, |column| column.affinity)
    }
}

/// Allocate the legacy query destination from facts already frozen in HIR.
///
/// The matching expression binding is installed at the same time so query
/// emission and expression lowering cannot choose different storage.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn prepare_subquery(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &SubqueryExpr,
) -> Result<PreparedSubquery> {
    let query_id = match expression {
        SubqueryExpr::Scalar { query, .. }
        | SubqueryExpr::Row { query }
        | SubqueryExpr::In { query, .. }
        | SubqueryExpr::Exists(query) => *query,
    };
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR subquery destination references missing query {query_id}"
        ))
    })?;
    let width = query.output.len();
    if width == 0 {
        return Err(LimboError::InternalError(format!(
            "HIR subquery {query_id} has no outputs"
        )));
    }

    let prepared = match expression {
        SubqueryExpr::Scalar { output, .. } => {
            if *output >= width {
                return Err(LimboError::InternalError(format!(
                    "HIR scalar subquery {query_id} output {output} exceeds query width {width}"
                )));
            }
            row_value_destination(program, width)
        }
        SubqueryExpr::Row { .. } => row_value_destination(program, width),
        SubqueryExpr::Exists(_) => {
            let register = program.alloc_register();
            DestinationBinding {
                destination: QueryDestination::ExistsSubqueryResult {
                    result_reg: register,
                },
                binding: SubqueryBinding::Exists { register },
            }
        }
        SubqueryExpr::In { comparison, .. } => {
            if comparison.components.len() != width {
                return Err(LimboError::InternalError(format!(
                    "HIR IN subquery {query_id} comparison width {} does not match query width {width}",
                    comparison.components.len()
                )));
            }
            let columns = query
                .output
                .iter()
                .zip(&comparison.components)
                .enumerate()
                .map(|(position, (output, component))| {
                    let output = document.output(*output).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR IN subquery {query_id} references missing output {output:?}"
                        ))
                    })?;
                    Ok(IndexColumn {
                        name: output.name.clone(),
                        order: SortOrder::Asc,
                        nulls_order: None,
                        pos_in_table: position,
                        collation: component
                            .collation
                            .as_ref()
                            .map(|collation| *collation.value()),
                        default: None,
                        expr: None,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let affinity_str = comparison
                .components
                .iter()
                .map(|component| component.affinity.aff_mask())
                .collect::<String>();
            let index = Arc::new(Index {
                columns,
                name: format!("ephemeral_index_hir_sub_{}", query_id.index()),
                table_name: String::new(),
                root_page: 0,
                unique: false,
                ephemeral: true,
                has_rowid: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            });
            let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            DestinationBinding {
                destination: QueryDestination::EphemeralIndex {
                    cursor_id: cursor,
                    index,
                    affinity_str: Some(Arc::new(affinity_str)),
                    is_delete: false,
                },
                binding: SubqueryBinding::InIndex { cursor },
            }
        }
    };

    program.bind_subquery(query_id, prepared.binding);
    Ok(PreparedSubquery {
        query: query_id,
        destination: prepared.destination,
        correlated: !query.captures.is_empty(),
    })
}

/// Emit the execution shell shared by all non-FROM expression subqueries.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn emit_prepared_subquery(
    program: &mut ProgramBuilder,
    prepared: &PreparedSubquery,
    emit_body: impl FnOnce(&mut ProgramBuilder, QueryId, &QueryDestination) -> Result<()>,
) -> Result<()> {
    program.nested(|program| {
        let explain_id = program.next_subquery_eqp_id();
        match &prepared.destination {
            QueryDestination::ExistsSubqueryResult { .. } => {}
            QueryDestination::RowValueSubqueryResult { .. } => {
                crate::emit_explain!(
                    program,
                    true,
                    EqpDetail::ScalarSubquery {
                        id: explain_id,
                        correlated: prepared.correlated,
                    }
                );
            }
            QueryDestination::EphemeralIndex { .. } => {
                crate::emit_explain!(
                    program,
                    true,
                    EqpDetail::ListSubquery {
                        id: explain_id,
                        correlated: prepared.correlated,
                    }
                );
            }
            destination => {
                return Err(LimboError::InternalError(format!(
                    "HIR expression subquery has invalid destination {destination:?}"
                )));
            }
        }

        let once_done = if prepared.correlated {
            None
        } else {
            let done = program.allocate_label();
            program.emit_insn(Insn::Once {
                target_pc_when_reentered: done,
            });
            Some(done)
        };

        match &prepared.destination {
            QueryDestination::ExistsSubqueryResult { result_reg } => {
                let return_register = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: return_register,
                    dest_end: None,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
                emit_body(program, prepared.query, &prepared.destination)?;
                program.emit_insn(Insn::Return {
                    return_reg: return_register,
                    can_fallthrough: true,
                });
            }
            QueryDestination::RowValueSubqueryResult {
                result_reg_start,
                num_regs,
            } => {
                let return_register = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: return_register,
                    dest_end: None,
                });
                for register in *result_reg_start..*result_reg_start + *num_regs {
                    program.emit_insn(Insn::Null {
                        dest: register,
                        dest_end: None,
                    });
                }
                emit_body(program, prepared.query, &prepared.destination)?;
                program.emit_insn(Insn::Return {
                    return_reg: return_register,
                    can_fallthrough: true,
                });
            }
            QueryDestination::EphemeralIndex { cursor_id, .. } => {
                program.emit_insn(Insn::OpenEphemeral {
                    cursor_id: *cursor_id,
                    is_table: false,
                });
                emit_body(program, prepared.query, &prepared.destination)?;
            }
            _ => unreachable!("expression subquery destination was checked"),
        }

        if !matches!(
            prepared.destination,
            QueryDestination::ExistsSubqueryResult { .. }
        ) {
            program.pop_current_parent_explain();
        }
        if let Some(once_done) = once_done {
            program.preassign_label_to_next_insn(once_done);
        }
        Ok(())
    })
}

/// Emit SELECT and VALUES bodies that do not read a row source.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn emit_query_body(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query_id: QueryId,
    destination: &QueryDestination,
) -> Result<()> {
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR lowering references missing query {query_id}"))
    })?;
    if query.blocks.len() != 1 || !query.compounds.is_empty() || !query.order_by.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR query {query_id} is not a supported non-FROM query body"
        )));
    }
    let block = &query.blocks[0];
    if block.from.is_some()
        || block.aggregate_count != 0
        || block.window_function_count != 0
        || !block.windows.is_empty()
    {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} is not a supported non-FROM query body",
            block.id
        )));
    }

    let done = program.allocate_label();
    let limit = initialize_limit(program, document, query.limit.as_ref(), done)?;
    let result = match &block.body {
        hir::QueryBlockBody::Select {
            distinctness: None,
            filter,
            grouping: None,
        } => {
            let start = query_output_registers(program, destination, &block.outputs)?;
            if let Some(filter) = filter {
                let emit_row = program.allocate_label();
                super::expr::translate_condition_expr(
                    program,
                    document,
                    filter,
                    ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true: emit_row,
                        jump_target_when_false: done,
                        jump_target_when_null: done,
                    },
                )?;
                program.preassign_label_to_next_insn(emit_row);
            }
            emit_before_row(program, limit, done);
            emit_query_row(
                program,
                document,
                destination,
                &block.outputs,
                block.outputs.iter().map(|output| &output.expr),
                start,
            )?;
            emit_after_row(
                program,
                limit,
                destination_stops_after_first_row(destination),
                done,
            );
            Ok(())
        }
        hir::QueryBlockBody::Values { rows } => {
            let start = query_output_registers(program, destination, &block.outputs)?;
            let stop_after_first = destination_stops_after_first_row(destination);
            for row in rows {
                let next_row = program.allocate_label();
                emit_before_row(program, limit, next_row);
                emit_query_row(
                    program,
                    document,
                    destination,
                    &block.outputs,
                    row.iter(),
                    start,
                )?;
                let needs_runtime_stop =
                    stop_after_first && limit.and_then(|limit| limit.offset).is_some();
                emit_after_row(program, limit, needs_runtime_stop, done);
                program.preassign_label_to_next_insn(next_row);
                if stop_after_first && !needs_runtime_stop {
                    break;
                }
            }
            Ok(())
        }
        _ => Err(LimboError::InternalError(format!(
            "HIR query block {:?} has unsupported non-FROM clauses",
            block.id
        ))),
    };
    program.preassign_label_to_next_insn(done);
    result
}

/// Emit one planned HIR query whose source loop is a full B-tree scan.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn emit_planned_query_body(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query_id: QueryId,
    destination: &QueryDestination,
) -> Result<()> {
    let document = &plan.document;
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR lowering references missing query {query_id}"))
    })?;
    let planned_query = plan.planned_query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR lowering references unplanned query {query_id}"
        ))
    })?;
    if query.blocks.len() != 1
        || planned_query.blocks.len() != 1
        || !query.compounds.is_empty()
        || !query.order_by.is_empty()
    {
        return Err(LimboError::InternalError(format!(
            "HIR query {query_id} is not a supported single-block query body"
        )));
    }
    let block = &query.blocks[0];
    let block_plan = &planned_query.blocks[0];
    if block_plan.block != block.id {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} has mismatched access plan {:?}",
            block.id, block_plan.block
        )));
    }
    if block.from.is_none() {
        return emit_query_body(program, document, query_id, destination);
    }
    emit_btree_loops(
        program,
        plan,
        query.limit.as_ref(),
        block,
        block_plan,
        destination,
    )
}

enum HirLoopAdvance {
    None,
    Cursor {
        direction: IterationDirection,
        fullscan: bool,
    },
    InSeek {
        state: InSeekLoop,
        index_backed: bool,
    },
    Virtual,
}

struct PreparedHirBtreeLoop<'a> {
    source: &'a hir::Source,
    operation: &'a HirBtreeOperation,
    index: Option<&'a Arc<Index>>,
    cursor: CursorID,
    table_cursor: Option<CursorID>,
    autoindex_source_cursor: Option<CursorID>,
    table_has_rowid: bool,
}

struct PreparedHirMaterializedLoop<'a> {
    source: &'a hir::Source,
    cursor: CursorID,
    table: Arc<BTreeTable>,
}

struct PreparedHirVirtualLoop<'a> {
    source: &'a hir::Source,
    operation: &'a HirVirtualTableOperation,
    cursor: CursorID,
}

#[derive(Clone, Copy)]
enum HirMaterializedQuery {
    Derived(QueryId),
    Cte { cte: hir::CteId, query: QueryId },
}

impl HirMaterializedQuery {
    fn query(self) -> QueryId {
        match self {
            Self::Derived(query) | Self::Cte { query, .. } => query,
        }
    }
}

enum PreparedHirLoop<'a> {
    BTree(PreparedHirBtreeLoop<'a>),
    Virtual(PreparedHirVirtualLoop<'a>),
    Materialized(PreparedHirMaterializedLoop<'a>),
}

impl PreparedHirLoop<'_> {
    fn cursor(&self) -> CursorID {
        match self {
            Self::BTree(prepared) => prepared.cursor,
            Self::Virtual(prepared) => prepared.cursor,
            Self::Materialized(prepared) => prepared.cursor,
        }
    }

    fn table_cursor(&self) -> Option<CursorID> {
        match self {
            Self::BTree(prepared) => prepared.table_cursor,
            Self::Virtual(_) => None,
            Self::Materialized(_) => None,
        }
    }
}

struct HirBtreeLoop {
    cursor: CursorID,
    loop_start: BranchOffset,
    /// Reject the current row, then run this loop's advance operation.
    next: BranchOffset,
    /// This access produced no row, so skip its advance operation.
    exhausted: BranchOffset,
    advance: HirLoopAdvance,
    left_join: Option<usize>,
}

struct HirLeftJoin {
    owner: SourceId,
    first_loop: usize,
    last_loop: usize,
    metadata: LeftJoinMetadata,
    null_cursors: Vec<CursorID>,
}

#[derive(Clone, Copy)]
enum HirPredicateKind {
    Join(SourceId),
    Where,
}

fn emit_btree_loops(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query_limit: Option<&hir::Limit>,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    destination: &QueryDestination,
) -> Result<()> {
    let document = &plan.document;
    if block.aggregate_count != 0 || block.window_function_count != 0 || !block.windows.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} is not a supported B-tree loop",
            block.id
        )));
    }
    let hir::QueryBlockBody::Select {
        distinctness: None,
        grouping: None,
        ..
    } = &block.body
    else {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} has unsupported B-tree loop clauses",
            block.id
        )));
    };
    if block_plan.loops.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} does not have a source loop",
            block.id
        )));
    }
    let from = block.from.as_ref().expect("FROM query block has a source");
    let mut left_join_owners = Vec::new();
    if !supported_btree_from(document, from, &mut left_join_owners) {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} contains an unsupported join",
            block.id
        )));
    }
    if block_plan.predicates.iter().any(|predicate| {
        predicate
            .from_outer_join
            .is_some_and(|source| !left_join_owners.contains(&source))
    }) {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} contains an unsupported outer-join predicate owner",
            block.id
        )));
    }

    let done = program.allocate_label();
    let limit = initialize_limit(program, document, query_limit, done)?;
    let prepared_loops = block_plan
        .loops
        .iter()
        .map(|planned_loop| prepare_hir_loop(program, plan, planned_loop))
        .collect::<Result<Vec<_>>>()?;
    let left_joins =
        prepare_hir_left_joins(program, block_plan, &prepared_loops, &left_join_owners)?;

    emit_ready_predicates(
        program,
        document,
        block_plan,
        None,
        done,
        HirPredicateKind::Where,
        &left_joins,
    )?;
    let mut loops = Vec::with_capacity(prepared_loops.len());
    for (loop_index, prepared_loop) in prepared_loops.iter().enumerate() {
        let mut starting_left_joins = left_joins
            .iter()
            .enumerate()
            .filter(|(_, left_join)| left_join.first_loop == loop_index);
        let left_join = starting_left_joins.next().map(|(index, left_join)| {
            left_join.metadata.reset(program);
            index
        });
        if starting_left_joins.next().is_some() {
            return Err(LimboError::InternalError(format!(
                "HIR loop {loop_index} starts overlapping LEFT JOIN boundaries"
            )));
        }
        let hir_loop = start_hir_loop(program, document, prepared_loop, left_join)?;
        for left_join in left_joins
            .iter()
            .filter(|left_join| left_join.last_loop == loop_index)
        {
            emit_ready_predicates(
                program,
                document,
                block_plan,
                Some(loop_index),
                hir_loop.next,
                HirPredicateKind::Join(left_join.owner),
                &left_joins,
            )?;
            left_join.metadata.mark_matched(program);
        }
        emit_ready_predicates(
            program,
            document,
            block_plan,
            Some(loop_index),
            hir_loop.next,
            HirPredicateKind::Where,
            &left_joins,
        )?;
        loops.push(hir_loop);
    }

    let innermost_next = loops.last().expect("loop list is non-empty").next;
    emit_before_row(program, limit, innermost_next);
    let start = query_output_registers(program, destination, &block.outputs)?;
    emit_query_row(
        program,
        document,
        destination,
        &block.outputs,
        block.outputs.iter().map(|output| &output.expr),
        start,
    )?;
    emit_after_row(
        program,
        limit,
        destination_stops_after_first_row(destination),
        done,
    );

    for hir_loop in loops.iter().rev() {
        close_hir_btree_loop(program, hir_loop, &left_joins);
    }
    program.preassign_label_to_next_insn(done);
    Ok(())
}

fn supported_btree_from(
    document: &HirDocument,
    from: &hir::From,
    left_join_owners: &mut Vec<SourceId>,
) -> bool {
    let Some(first) = document.source(from.first) else {
        return false;
    };
    if let hir::SourceKind::FromGroup(group) = &first.kind {
        if !supported_btree_from(document, &group.from, left_join_owners) {
            return false;
        }
    }
    for join in &from.joins {
        let Some(source) = document.source(join.right) else {
            return false;
        };
        match join.kind {
            hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross => {
                if let hir::SourceKind::FromGroup(group) = &source.kind {
                    if !supported_btree_from(document, &group.from, left_join_owners) {
                        return false;
                    }
                }
            }
            hir::JoinKind::Left => {
                if let hir::SourceKind::FromGroup(group) = &source.kind {
                    if !ordinary_btree_from(document, &group.from) {
                        return false;
                    }
                }
                left_join_owners.push(join.right);
            }
            hir::JoinKind::Right | hir::JoinKind::Full => return false,
        }
    }
    true
}

fn ordinary_btree_from(document: &HirDocument, from: &hir::From) -> bool {
    from.joins.iter().all(|join| {
        matches!(
            join.kind,
            hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross
        )
    }) && std::iter::once(from.first)
        .chain(from.joins.iter().map(|join| join.right))
        .all(|source| {
            document.source(source).is_some_and(|source| {
                if let hir::SourceKind::FromGroup(group) = &source.kind {
                    ordinary_btree_from(document, &group.from)
                } else {
                    true
                }
            })
        })
}

fn prepare_hir_left_joins(
    program: &mut ProgramBuilder,
    block_plan: &HirQueryBlockPlan,
    prepared_loops: &[PreparedHirLoop<'_>],
    owners: &[SourceId],
) -> Result<Vec<HirLeftJoin>> {
    owners
        .iter()
        .map(|owner| {
            let loop_indices = if let Some(group) = block_plan
                .groups
                .iter()
                .find(|group| group.source == *owner)
            {
                block_plan
                    .loops
                    .iter()
                    .enumerate()
                    .filter_map(|(loop_index, planned_loop)| {
                        group
                            .source_range
                            .contains(&planned_loop.source_position)
                            .then_some(loop_index)
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![block_plan
                    .loops
                    .iter()
                    .position(|planned_loop| planned_loop.source == *owner)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR LEFT JOIN owner {owner} has no physical loop"
                        ))
                    })?]
            };
            let Some((&first_loop, &last_loop)) = loop_indices.first().zip(loop_indices.last())
            else {
                return Err(LimboError::InternalError(format!(
                    "HIR LEFT JOIN group {owner} has no physical loops"
                )));
            };
            if loop_indices.len() != last_loop - first_loop + 1 {
                return Err(LimboError::InternalError(format!(
                    "HIR LEFT JOIN group {owner} is not contiguous in physical loop order"
                )));
            }
            let mut null_cursors = Vec::new();
            for prepared in &prepared_loops[first_loop..=last_loop] {
                null_cursors.push(prepared.cursor());
                if let Some(table_cursor) = prepared.table_cursor() {
                    null_cursors.push(table_cursor);
                }
            }
            Ok(HirLeftJoin {
                owner: *owner,
                first_loop,
                last_loop,
                metadata: LeftJoinMetadata::new(program),
                null_cursors,
            })
        })
        .collect()
}

fn prepare_hir_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    planned_loop: &'a HirPlannedLoop,
) -> Result<PreparedHirLoop<'a>> {
    match &planned_loop.access {
        HirSourceAccess::BTree { .. } => Ok(PreparedHirLoop::BTree(prepare_hir_btree_loop(
            program,
            &plan.document,
            planned_loop,
        )?)),
        HirSourceAccess::Virtual(operation) => Ok(PreparedHirLoop::Virtual(
            prepare_hir_virtual_loop(program, &plan.document, planned_loop.source, operation)?,
        )),
        HirSourceAccess::Derived { query } => Ok(PreparedHirLoop::Materialized(
            prepare_hir_materialized_loop(
                program,
                plan,
                planned_loop.source,
                HirMaterializedQuery::Derived(*query),
            )?,
        )),
        HirSourceAccess::Cte {
            cte,
            query,
            materialization: HirCteMaterialization::PerReference,
        } => Ok(PreparedHirLoop::Materialized(
            prepare_hir_materialized_loop(
                program,
                plan,
                planned_loop.source,
                HirMaterializedQuery::Cte {
                    cte: *cte,
                    query: *query,
                },
            )?,
        )),
        HirSourceAccess::Cte {
            cte,
            query,
            materialization: HirCteMaterialization::Shared | HirCteMaterialization::Explicit,
        } => Ok(PreparedHirLoop::Materialized(prepare_hir_shared_cte_loop(
            program,
            plan,
            planned_loop.source,
            *cte,
            *query,
        )?)),
        _ => Err(LimboError::InternalError(format!(
            "HIR source {} does not use a supported source loop",
            planned_loop.source
        ))),
    }
}

fn prepare_hir_virtual_loop<'a>(
    program: &mut ProgramBuilder,
    document: &'a HirDocument,
    source_id: SourceId,
    operation: &'a HirVirtualTableOperation,
) -> Result<PreparedHirVirtualLoop<'a>> {
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR virtual-table loop references missing source {source_id}"
        ))
    })?;
    let table = match &source.kind {
        hir::SourceKind::Table(table) | hir::SourceKind::TableFunction { table, .. } => table,
        _ => {
            return Err(LimboError::InternalError(format!(
                "HIR virtual-table loop source {} is not a table",
                source.id
            )))
        }
    };
    let Table::Virtual(table) = table.value() else {
        return Err(LimboError::InternalError(format!(
            "HIR virtual-table loop source {} is not a virtual table",
            source.id
        )));
    };
    let cursor = program.alloc_cursor_id(CursorType::VirtualTable(table.clone()));
    program.emit_insn(Insn::VOpen { cursor_id: cursor });
    program.bind_source(source.id, SourceBinding::Virtual { cursor });
    Ok(PreparedHirVirtualLoop {
        source,
        operation,
        cursor,
    })
}

fn prepare_hir_shared_cte_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    cte: hir::CteId,
    query: QueryId,
) -> Result<PreparedHirMaterializedLoop<'a>> {
    let materialized_query = HirMaterializedQuery::Cte { cte, query };
    let source = validate_hir_materialized_query(plan, source_id, materialized_query)?;
    if let Some(shared) = program.hir_materialized_cte(cte).cloned() {
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(shared.table.clone()));
        program.emit_insn(Insn::OpenDup {
            new_cursor_id: cursor,
            original_cursor_id: shared.cursor_id,
        });
        program.bind_source(
            source_id,
            SourceBinding::BTree {
                scan_cursor: cursor,
                table_cursor: None,
            },
        );
        return Ok(PreparedHirMaterializedLoop {
            source,
            cursor,
            table: shared.table,
        });
    }

    let prepared = prepare_hir_materialized_loop(program, plan, source_id, materialized_query)?;
    program.register_hir_materialized_cte(
        cte,
        MaterializedCteInfo {
            cursor_id: prepared.cursor,
            table: prepared.table.clone(),
            num_columns: prepared.source.columns.len(),
        },
    );
    Ok(prepared)
}

fn validate_hir_materialized_query<'a>(
    plan: &'a HirPlan,
    source_id: SourceId,
    materialized_query: HirMaterializedQuery,
) -> Result<&'a hir::Source> {
    let document = &plan.document;
    let query_id = materialized_query.query();
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR materialized loop references missing source {source_id}"
        ))
    })?;
    let source_matches = match materialized_query {
        HirMaterializedQuery::Derived(query) => {
            matches!(source.kind, hir::SourceKind::Derived(source_query) if source_query == query)
        }
        HirMaterializedQuery::Cte { cte, .. } => {
            matches!(source.kind, hir::SourceKind::Cte(source_cte) if source_cte == cte)
        }
    };
    if !source_matches {
        return Err(LimboError::InternalError(format!(
            "HIR materialized loop source {source_id} does not match query {query_id}"
        )));
    }
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR materialized source {source_id} references missing query {query_id}"
        ))
    })?;
    if !query.captures.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR materialized source {source_id} is correlated"
        )));
    }
    if source.columns.len() != query.output.len() {
        return Err(LimboError::InternalError(format!(
            "HIR materialized source {source_id} width {} does not match query {query_id} width {}",
            source.columns.len(),
            query.output.len()
        )));
    }
    Ok(source)
}

fn prepare_hir_materialized_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    materialized_query: HirMaterializedQuery,
) -> Result<PreparedHirMaterializedLoop<'a>> {
    let query_id = materialized_query.query();
    let source = validate_hir_materialized_query(plan, source_id, materialized_query)?;

    let columns = source
        .columns
        .iter()
        .map(|column| {
            let storage = column.type_fact.storage.unwrap_or(Type::Null);
            let ty_str = match column.affinity {
                Affinity::Blob => "BLOB",
                Affinity::Text => "TEXT",
                Affinity::Numeric => "NUMERIC",
                Affinity::Integer => "INTEGER",
                Affinity::Real => "REAL",
                Affinity::None => "",
            };
            Column::new(
                Some(column.name.clone()),
                ty_str.to_string(),
                None,
                None,
                storage,
                column
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value()),
                ColDef {
                    hidden: column.hidden,
                    ..ColDef::default()
                },
            )
        })
        .collect();
    let table = Arc::new(BTreeTable::new(
        0,
        source.name.clone(),
        Vec::new(),
        columns,
        BTreeCharacteristics::HAS_ROWID,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    ));
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let materialized = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: materialized,
    });
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: true,
    });
    emit_planned_query_body(
        program,
        plan,
        query_id,
        &QueryDestination::EphemeralTable {
            cursor_id: cursor,
            table: table.clone(),
            rowid_mode: EphemeralRowidMode::Auto,
        },
    )?;
    program.preassign_label_to_next_insn(materialized);
    program.bind_source(
        source_id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor: None,
        },
    );
    Ok(PreparedHirMaterializedLoop {
        source,
        cursor,
        table,
    })
}

fn prepare_hir_btree_loop<'a>(
    program: &mut ProgramBuilder,
    document: &'a HirDocument,
    planned_loop: &'a HirPlannedLoop,
) -> Result<PreparedHirBtreeLoop<'a>> {
    let HirSourceAccess::BTree {
        operation,
        table_lookup,
    } = &planned_loop.access
    else {
        return Err(LimboError::InternalError(format!(
            "HIR source {} does not use a B-tree loop",
            planned_loop.source
        )));
    };
    let index = match operation {
        HirBtreeOperation::Scan { index, .. }
        | HirBtreeOperation::Seek { index, .. }
        | HirBtreeOperation::InSeek { index, .. } => index.as_ref(),
        HirBtreeOperation::RowidEq { .. } => None,
    };
    if index.is_some_and(|index| index.ephemeral)
        && !matches!(operation, HirBtreeOperation::Seek { .. })
    {
        return Err(LimboError::InternalError(
            "HIR automatic indexes require a seek operation".to_string(),
        ));
    }
    let source = document.source(planned_loop.source).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR B-tree loop references missing source {}",
            planned_loop.source
        ))
    })?;
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(format!(
            "HIR B-tree loop source {} is not a table",
            source.id
        )));
    };
    let Table::BTree(table) = table.value() else {
        return Err(LimboError::InternalError(format!(
            "HIR B-tree loop source {} is not a B-tree table",
            source.id
        )));
    };
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR B-tree loop source {} has no database",
            source.id
        ))
    })?;
    let database_snapshot = document.database(database).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR B-tree loop source {} references missing database {database:?}",
            source.id
        ))
    })?;
    program.begin_read_on_database(database.index(), database_snapshot.schema_version)?;
    let (cursor, table_cursor, autoindex_source_cursor) = if let Some(index) = index {
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        let table_cursor = match table_lookup {
            BtreeTableLookup::ScanOnly => None,
            BtreeTableLookup::TableRequired => {
                Some(program.alloc_cursor_id(CursorType::BTreeTable(table.clone())))
            }
        };
        let autoindex_source_cursor =
            if index.ephemeral {
                Some(table_cursor.unwrap_or_else(|| {
                    program.alloc_cursor_id(CursorType::BTreeTable(table.clone()))
                }))
            } else {
                None
            };
        (index_cursor, table_cursor, autoindex_source_cursor)
    } else {
        if *table_lookup != BtreeTableLookup::ScanOnly {
            return Err(LimboError::InternalError(format!(
                "HIR table scan source {} unexpectedly requires a second table cursor",
                source.id
            )));
        }
        (
            program.alloc_cursor_id(CursorType::BTreeTable(table.clone())),
            None,
            None,
        )
    };
    program.bind_source(
        source.id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor,
        },
    );
    if index.is_none_or(|index| !index.ephemeral) {
        program.emit_insn(Insn::OpenRead {
            cursor_id: cursor,
            root_page: index.map_or(table.root_page, |index| index.root_page),
            db: database.index(),
        });
    }
    if let Some(source_cursor) = autoindex_source_cursor.or(table_cursor) {
        program.emit_insn(Insn::OpenRead {
            cursor_id: source_cursor,
            root_page: table.root_page,
            db: database.index(),
        });
    }

    Ok(PreparedHirBtreeLoop {
        source,
        operation,
        index,
        cursor,
        table_cursor,
        autoindex_source_cursor,
        table_has_rowid: table.has_rowid,
    })
}

fn build_hir_autoindex(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirBtreeLoop<'_>,
    seek_def: &HirSeekDef,
) -> Result<bool> {
    let Some(index) = prepared.index.filter(|index| index.ephemeral) else {
        return Ok(false);
    };
    let table_cursor = prepared
        .autoindex_source_cursor
        .expect("automatic index has a source table cursor");
    let num_seek_keys = seek_def.size(&seek_def.start);
    let lowering = HirSeekExpressionLowering {
        document,
        source: prepared.source,
    };
    let has_null_matching_key = seek_def.prefix.iter().take(num_seek_keys).any(|component| {
        component
            .eq
            .as_ref()
            .is_some_and(|(operator, expression, _)| {
                lowering.is_null_matching(*operator, expression)
            })
    });
    let affinity_str = plan::synthesized_seek_affinity_str(index, seek_def);

    // Generated expressions must read the base-table row while the automatic
    // index is being populated. Restore the planned index binding before the seek.
    program.bind_source(
        prepared.source.id,
        SourceBinding::BTree {
            scan_cursor: table_cursor,
            table_cursor: None,
        },
    );
    let result = emit_autoindex(
        program,
        AutoIndexBuild {
            index,
            table_cursor_id: table_cursor,
            index_cursor_id: prepared.cursor,
            table_has_rowid: prepared.table_has_rowid,
            num_seek_keys,
            has_null_matching_key,
            seek_def,
            affinity_str: affinity_str.as_ref(),
        },
        |program, column, target| {
            let Some(expression) = prepared.source.generated_expressions.get(column) else {
                return Err(LimboError::InternalError(format!(
                    "automatic index column {column} is outside source {}",
                    prepared.source.id
                )));
            };
            match expression {
                hir::ColumnReadExpression::Absent => {
                    program.emit_column_or_rowid(table_cursor, column, target);
                    Ok(())
                }
                hir::ColumnReadExpression::Planned(expression) => {
                    super::expr::translate_expr_no_constant_opt(
                        program, document, expression, target,
                    )?;
                    Ok(())
                }
                hir::ColumnReadExpression::NotRequired => Err(LimboError::InternalError(format!(
                    "automatic index requires unplanned generated column {column} from source {}",
                    prepared.source.id
                ))),
            }
        },
    );
    program.bind_source(
        prepared.source.id,
        SourceBinding::BTree {
            scan_cursor: prepared.cursor,
            table_cursor: prepared.table_cursor,
        },
    );
    Ok(result?.use_bloom_filter)
}

fn start_hir_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    match prepared {
        PreparedHirLoop::BTree(prepared) => {
            start_hir_btree_loop(program, document, prepared, left_join)
        }
        PreparedHirLoop::Virtual(prepared) => {
            start_hir_virtual_loop(program, document, prepared, left_join)
        }
        PreparedHirLoop::Materialized(prepared) => {
            if !matches!(
                program.source_binding(prepared.source.id),
                Some(SourceBinding::BTree {
                    scan_cursor,
                    table_cursor: None,
                }) if *scan_cursor == prepared.cursor
            ) {
                return Err(LimboError::InternalError(format!(
                    "HIR materialized source {} lost its cursor binding",
                    prepared.source.id
                )));
            }
            let loop_start = program.allocate_label();
            let next = program.allocate_label();
            let exhausted = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: prepared.cursor,
                pc_if_empty: exhausted,
            });
            program.preassign_label_to_next_insn(loop_start);
            Ok(HirBtreeLoop {
                cursor: prepared.cursor,
                loop_start,
                next,
                exhausted,
                advance: HirLoopAdvance::Cursor {
                    direction: IterationDirection::Forwards,
                    fullscan: true,
                },
                left_join,
            })
        }
    }
}

fn start_hir_virtual_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirVirtualLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    if !matches!(
        program.source_binding(prepared.source.id),
        Some(SourceBinding::Virtual { cursor }) if *cursor == prepared.cursor
    ) {
        return Err(LimboError::InternalError(format!(
            "HIR virtual-table source {} lost its cursor binding",
            prepared.source.id
        )));
    }
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let exhausted = program.allocate_label();
    let args_reg = program.alloc_registers(prepared.operation.arguments.len());
    for (index, argument) in prepared.operation.arguments.iter().enumerate() {
        super::expr::translate_expr(program, document, argument, args_reg + index)?;
    }
    let idx_str = prepared.operation.idx_str.as_ref().map(|value| {
        let register = program.alloc_register();
        program.emit_insn(Insn::String8 {
            dest: register,
            value: value.clone(),
        });
        register
    });
    program.emit_insn(Insn::VFilter {
        cursor_id: prepared.cursor,
        pc_if_empty: exhausted,
        arg_count: prepared.operation.arguments.len(),
        args_reg,
        idx_str,
        idx_num: prepared.operation.idx_num as usize,
    });
    program.preassign_label_to_next_insn(loop_start);
    Ok(HirBtreeLoop {
        cursor: prepared.cursor,
        loop_start,
        next,
        exhausted,
        advance: HirLoopAdvance::Virtual,
        left_join,
    })
}

fn start_hir_btree_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirBtreeLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let exhausted = program.allocate_label();
    let advance = match prepared.operation {
        HirBtreeOperation::Scan { iter_dir, .. } => {
            match iter_dir {
                IterationDirection::Forwards => program.emit_insn(Insn::Rewind {
                    cursor_id: prepared.cursor,
                    pc_if_empty: exhausted,
                }),
                IterationDirection::Backwards => program.emit_insn(Insn::Last {
                    cursor_id: prepared.cursor,
                    pc_if_empty: exhausted,
                }),
            }
            program.preassign_label_to_next_insn(loop_start);
            HirLoopAdvance::Cursor {
                direction: *iter_dir,
                fullscan: true,
            }
        }
        HirBtreeOperation::RowidEq { cmp_expr } => {
            let key = program.alloc_register();
            super::expr::translate_expr(program, document, cmp_expr, key)?;
            program.emit_insn(Insn::SeekRowid {
                cursor_id: prepared.cursor,
                src_reg: key,
                target_pc: exhausted,
            });
            HirLoopAdvance::None
        }
        HirBtreeOperation::Seek { seek_def, .. } => {
            let use_bloom_filter = build_hir_autoindex(program, document, prepared, seek_def)?;
            let key_registers = seek_def
                .size(&seek_def.start)
                .max(seek_def.size(&seek_def.end));
            let start_register = program.alloc_registers(key_registers);
            SeekEmitter::with_lowering(
                program,
                seek_def,
                HirSeekExpressionLowering {
                    document,
                    source: prepared.source,
                },
                prepared.cursor,
                start_register,
                exhausted,
                prepared.index,
            )
            .emit(loop_start, use_bloom_filter)?;
            HirLoopAdvance::Cursor {
                direction: seek_def.iter_dir,
                fullscan: false,
            }
        }
        HirBtreeOperation::InSeek { source, .. } => {
            let source_cursor =
                open_hir_in_seek_source_cursor(program, document, prepared.index, source)?;
            let state = emit_in_seek_start(
                program,
                source_cursor,
                prepared.cursor,
                prepared.table_cursor,
                prepared.index.is_some(),
                loop_start,
                exhausted,
            );
            HirLoopAdvance::InSeek {
                state,
                index_backed: prepared.index.is_some(),
            }
        }
    };
    if !matches!(prepared.operation, HirBtreeOperation::InSeek { .. }) {
        if let Some(table_cursor) = prepared.table_cursor {
            program.emit_insn(Insn::DeferredSeek {
                index_cursor_id: prepared.cursor,
                table_cursor_id: table_cursor,
            });
        }
    }
    Ok(HirBtreeLoop {
        cursor: prepared.cursor,
        loop_start,
        next,
        exhausted,
        advance,
        left_join,
    })
}

fn emit_ready_predicates(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block_plan: &HirQueryBlockPlan,
    loop_index: Option<usize>,
    fail: BranchOffset,
    kind: HirPredicateKind,
    left_joins: &[HirLeftJoin],
) -> Result<()> {
    for predicate in block_plan.predicates.iter().filter(|term| !term.consumed) {
        let selected = match kind {
            HirPredicateKind::Join(source) => predicate.from_outer_join == Some(source),
            HirPredicateKind::Where => predicate.from_outer_join.is_none(),
        };
        if !selected {
            continue;
        }
        let references_ready_at =
            predicate_loop(program, document, &predicate.expr, &block_plan.loops)?;
        let evaluate_at = match kind {
            HirPredicateKind::Join(source) => {
                let owner = left_joins
                    .iter()
                    .find(|left_join| left_join.owner == source)
                    .map(|left_join| left_join.last_loop)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR outer-join predicate owner {source} has no LEFT JOIN boundary"
                        ))
                    })?;
                if references_ready_at.is_some_and(|ready| ready > owner) {
                    return Err(LimboError::InternalError(format!(
                        "HIR outer-join predicate owned by {source} references a later loop"
                    )));
                }
                Some(owner)
            }
            HirPredicateKind::Where => references_ready_at.map(|ready| {
                left_joins.iter().fold(ready, |ready, left_join| {
                    if left_join.first_loop <= ready && ready <= left_join.last_loop {
                        left_join.last_loop
                    } else {
                        ready
                    }
                })
            }),
        };
        if evaluate_at != loop_index {
            continue;
        }
        let passed = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            &predicate.expr,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: passed,
                jump_target_when_false: fail,
                jump_target_when_null: fail,
            },
        )?;
        program.preassign_label_to_next_insn(passed);
    }
    Ok(())
}

fn predicate_loop(
    program: &ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    loops: &[HirPlannedLoop],
) -> Result<Option<usize>> {
    let mut eval_at = None;
    let mut missing = None;
    let mut missing_query = None;
    let mut record_source = |source| {
        if let Some(position) = loops.iter().position(|hir_loop| hir_loop.source == source) {
            eval_at = Some(eval_at.map_or(position, |current: usize| current.max(position)));
        } else if program.source_binding(source).is_none() {
            missing = Some(source);
        }
    };
    expression.for_each(&mut |expression| match expression {
        hir::Expr::Column(reference) => record_source(reference.source),
        hir::Expr::RowId(source) => record_source(*source),
        hir::Expr::Subquery(subquery) => {
            let query = match subquery {
                SubqueryExpr::Scalar { query, .. }
                | SubqueryExpr::Row { query }
                | SubqueryExpr::In { query, .. }
                | SubqueryExpr::Exists(query) => *query,
            };
            if let Some(query) = document.query(query) {
                for source in &query.captures {
                    record_source(*source);
                }
            } else {
                missing_query = Some(query);
            }
        }
        _ => {}
    });
    if let Some(query) = missing_query {
        return Err(LimboError::InternalError(format!(
            "HIR predicate references missing subquery {query}"
        )));
    }
    if let Some(source) = missing {
        return Err(LimboError::InternalError(format!(
            "HIR predicate references source {source} outside its loop plan"
        )));
    }
    Ok(eval_at)
}

fn close_hir_btree_loop(
    program: &mut ProgramBuilder,
    hir_loop: &HirBtreeLoop,
    left_joins: &[HirLeftJoin],
) {
    program.preassign_label_to_next_insn(hir_loop.next);
    match &hir_loop.advance {
        HirLoopAdvance::None => {}
        HirLoopAdvance::Cursor {
            direction,
            fullscan,
        } => match direction {
            IterationDirection::Forwards => program.emit_insn(Insn::Next {
                cursor_id: hir_loop.cursor,
                pc_if_next: hir_loop.loop_start,
                fullscan: *fullscan,
            }),
            IterationDirection::Backwards => program.emit_insn(Insn::Prev {
                cursor_id: hir_loop.cursor,
                pc_if_prev: hir_loop.loop_start,
                fullscan: *fullscan,
            }),
        },
        HirLoopAdvance::InSeek {
            state,
            index_backed,
        } => emit_in_seek_advance(
            program,
            state,
            hir_loop.cursor,
            *index_backed,
            hir_loop.loop_start,
        ),
        HirLoopAdvance::Virtual => program.emit_insn(Insn::VNext {
            cursor_id: hir_loop.cursor,
            pc_if_next: hir_loop.loop_start,
        }),
    }
    program.preassign_label_to_next_insn(hir_loop.exhausted);
    if let Some(left_join) = hir_loop.left_join.map(|index| &left_joins[index]) {
        let finished = left_join.metadata.begin_unmatched_row(program);
        for cursor in &left_join.null_cursors {
            program.emit_insn(Insn::NullRow { cursor_id: *cursor });
        }
        left_join.metadata.finish_unmatched_row(program, finished);
    }
}

fn initialize_limit(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    limit: Option<&hir::Limit>,
    done: BranchOffset,
) -> Result<Option<QueryLimitRegisters>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let limit_register = program.alloc_register();
    emit_limit_value(program, document, &limit.limit, limit_register)?;

    let offset = if let Some(offset) = &limit.offset {
        let offset_register = program.alloc_register();
        emit_offset_value(program, document, offset, offset_register)?;
        program.emit_insn(Insn::MustBeInt {
            reg: offset_register,
            target_pc: None,
        });
        let combined_register = program.alloc_register();
        program.emit_insn(Insn::OffsetLimit {
            limit_reg: limit_register,
            offset_reg: offset_register,
            combined_reg: combined_register,
        });
        Some(offset_register)
    } else {
        None
    };
    program.emit_insn(Insn::IfNot {
        reg: limit_register,
        target_pc: done,
        jump_if_null: false,
    });
    Ok(Some(QueryLimitRegisters {
        limit: limit_register,
        offset,
    }))
}

fn emit_limit_value(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    target: usize,
) -> Result<()> {
    match expression {
        hir::Expr::Literal(Literal::Numeric(value)) => match parse_numeric_literal(value)? {
            Value::Numeric(Numeric::Integer(value)) => {
                program.emit_insn(Insn::Integer {
                    value,
                    dest: target,
                });
            }
            Value::Numeric(Numeric::Float(value)) => {
                program.emit_insn(Insn::Real {
                    value: value.into(),
                    dest: target,
                });
                program.emit_insn(Insn::MustBeInt {
                    reg: target,
                    target_pc: None,
                });
            }
            _ => unreachable!("numeric parser returns a numeric value"),
        },
        _ => {
            super::expr::translate_expr(program, document, expression, target)?;
            program.emit_insn(Insn::MustBeInt {
                reg: target,
                target_pc: None,
            });
        }
    }
    Ok(())
}

fn emit_offset_value(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    target: usize,
) -> Result<()> {
    match expression {
        hir::Expr::Literal(Literal::Numeric(value)) => match parse_numeric_literal(value)? {
            Value::Numeric(Numeric::Integer(value)) => {
                program.emit_insn(Insn::Integer {
                    value,
                    dest: target,
                });
            }
            Value::Numeric(Numeric::Float(value)) => {
                program.emit_insn(Insn::Real {
                    value: value.into(),
                    dest: target,
                });
                program.emit_insn(Insn::MustBeInt {
                    reg: target,
                    target_pc: None,
                });
            }
            _ => unreachable!("numeric parser returns a numeric value"),
        },
        _ => {
            super::expr::translate_expr(program, document, expression, target)?;
        }
    }
    Ok(())
}

fn emit_before_row(
    program: &mut ProgramBuilder,
    limit: Option<QueryLimitRegisters>,
    skip_row: BranchOffset,
) {
    let Some(offset) = limit.and_then(|limit| limit.offset) else {
        return;
    };
    program.emit_insn(Insn::IfPos {
        reg: offset,
        target_pc: skip_row,
        decrement_by: 1,
    });
}

fn emit_after_row(
    program: &mut ProgramBuilder,
    limit: Option<QueryLimitRegisters>,
    stop_after_first: bool,
    done: BranchOffset,
) {
    if let Some(limit) = limit {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit.limit,
            target_pc: done,
        });
    }
    if stop_after_first {
        program.emit_insn(Insn::Goto { target_pc: done });
    }
}

fn query_output_registers(
    program: &mut ProgramBuilder,
    destination: &QueryDestination,
    outputs: &[hir::Output],
) -> Result<usize> {
    if outputs.is_empty() {
        return Err(LimboError::InternalError(
            "HIR query has no outputs".to_string(),
        ));
    }
    Ok(
        if matches!(destination, QueryDestination::ExistsSubqueryResult { .. }) {
            0
        } else {
            program.alloc_registers(outputs.len())
        },
    )
}

fn destination_stops_after_first_row(destination: &QueryDestination) -> bool {
    matches!(
        destination,
        QueryDestination::ExistsSubqueryResult { .. }
            | QueryDestination::RowValueSubqueryResult { .. }
    )
}

fn emit_query_row<'expr>(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    destination: &QueryDestination,
    outputs: &[hir::Output],
    expressions: impl ExactSizeIterator<Item = &'expr hir::Expr>,
    start: usize,
) -> Result<()> {
    if expressions.len() != outputs.len() {
        return Err(LimboError::InternalError(format!(
            "HIR row width {} does not match output width {}",
            expressions.len(),
            outputs.len()
        )));
    }
    if matches!(destination, QueryDestination::ExistsSubqueryResult { .. }) {
        for expression in expressions {
            super::expr::register_parameters(program, expression);
        }
    } else {
        for (position, expression) in expressions.enumerate() {
            super::expr::translate_expr_no_constant_opt(
                program,
                document,
                expression,
                start + position,
            )?;
            program.bind_output(outputs[position].id, start + position);
        }
        emit_array_results(program, outputs, start);
    }
    emit_columns_to_destination(program, destination, start, outputs.len())?;
    Ok(())
}

fn emit_array_results(program: &mut ProgramBuilder, outputs: &[hir::Output], start: usize) {
    for (position, output) in outputs.iter().enumerate() {
        if !output.type_fact.is_array() {
            continue;
        }
        let register = start + position;
        let skip = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: register,
            target_pc: skip,
        });
        program.emit_insn(Insn::ArrayDecode { reg: register });
        program.preassign_label_to_next_insn(skip);
    }
}

fn row_value_destination(program: &mut ProgramBuilder, width: usize) -> DestinationBinding {
    let start = program.alloc_registers(width);
    DestinationBinding {
        destination: QueryDestination::RowValueSubqueryResult {
            result_reg_start: start,
            num_regs: width,
        },
        binding: SubqueryBinding::RowValue {
            start,
            count: width,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dialect::SqliteDialect,
        schema::{BTreeTable, Schema},
        translate::{
            optimizer::CostModelParams,
            semantic::{
                catalog::{SemanticCatalog, SemanticCatalogDatabase},
                context::DoubleQuotedDml,
                hir::{self, Expr, HirRoot},
                SemanticOptions, SemanticRootInput,
            },
            semantic_to_plan::HirPlan,
        },
        vdbe::builder::{ProgramBuilderOpts, QueryMode},
        SymbolTable, MAIN_DB_ID,
    };
    use turso_parser::parser::Parser;

    fn program() -> ProgramBuilder {
        program_with_mode(QueryMode::Normal)
    }

    fn program_with_mode(mode: QueryMode) -> ProgramBuilder {
        ProgramBuilder::new(mode, None, ProgramBuilderOpts::new(0, 4, 0))
    }

    fn analyze_sql_with_schema(mut schema: Schema, sql: &str) -> (HirDocument, Arc<Schema>) {
        schema
            .resolve_all_custom_type_affinities()
            .expect("custom affinities resolve");
        let schema = Arc::new(schema);
        let catalog = SemanticCatalog {
            databases: vec![SemanticCatalogDatabase {
                id: hir::DatabaseId::new(MAIN_DB_ID),
                name: "main".to_string(),
                schema: schema.clone(),
            }],
            unqualified_database_search_path: vec![hir::DatabaseId::new(MAIN_DB_ID)],
        };
        let command = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains a statement");
        let turso_parser::ast::Cmd::Stmt(statement) = command else {
            panic!("SQL contains a statement command");
        };
        let document = crate::translate::semantic::analyze_root(
            &catalog,
            &SymbolTable::new(),
            SemanticOptions {
                dialect: Arc::new(SqliteDialect),
                custom_types_enabled: true,
                dqs_dml: DoubleQuotedDml::Enabled,
            },
            SemanticRootInput::Statement(&statement),
        )
        .map(|document| {
            document.validate().expect("HIR validates");
            document
        })
        .expect("SQL analyzes");
        (document, schema)
    }

    fn analyze_sql(schema: Schema, sql: &str) -> HirDocument {
        analyze_sql_with_schema(schema, sql).0
    }

    fn analyze_plan(schema: Schema, sql: &str) -> HirPlan {
        let (document, schema) = analyze_sql_with_schema(schema, sql);
        HirPlan::build(Arc::new(document), &schema, &CostModelParams::default())
            .expect("HIR query plans")
    }

    fn indexed_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value INTEGER, extra TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        schema
            .add_index(Arc::new(Index {
                name: "items_value".to_string(),
                table_name: "items".to_string(),
                root_page: 3,
                columns: vec![IndexColumn {
                    name: "value".to_string(),
                    order: SortOrder::Asc,
                    nulls_order: None,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                    expr: None,
                }],
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }))
            .expect("fixed index name is unique");
        schema
    }

    fn rowid_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        schema
    }

    fn joined_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE right_items(value INTEGER, extra TEXT)", 3)
                    .expect("right table schema parses"),
            ))
            .expect("right table name is unique");
        schema
            .add_index(Arc::new(Index {
                name: "right_items_value".to_string(),
                table_name: "right_items".to_string(),
                root_page: 4,
                columns: vec![IndexColumn {
                    name: "value".to_string(),
                    order: SortOrder::Asc,
                    nulls_order: None,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                    expr: None,
                }],
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }))
            .expect("right index name is unique");
        schema
    }

    fn automatic_join_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE right_items(value INTEGER, extra TEXT)", 3)
                    .expect("right table schema parses"),
            ))
            .expect("right table name is unique");
        schema.analyze_stats.table_stats_mut("left_items").row_count = Some(1_000);
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(1);
        schema
    }

    fn automatic_generated_join_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE right_items(\
                         value INTEGER, \
                         generated INTEGER AS (value + 1) VIRTUAL, \
                         extra TEXT\
                     )",
                    3,
                )
                .expect("generated table schema parses"),
            ))
            .expect("right table name is unique");
        schema.analyze_stats.table_stats_mut("left_items").row_count = Some(1_000);
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(1);
        schema
    }

    fn left_join_with_tail_schema() -> Schema {
        let mut schema = automatic_join_schema();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE tail_items(value INTEGER, extra TEXT)", 4)
                    .expect("tail table schema parses"),
            ))
            .expect("tail table name is unique");
        schema
    }

    fn root_expression(document: &HirDocument) -> &Expr {
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        &document.queries[root.query.index()].blocks[0].outputs[0].expr
    }

    fn root_query(document: &HirDocument) -> QueryId {
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        root.query
    }

    fn root_subquery(document: &HirDocument) -> &SubqueryExpr {
        match root_expression(document) {
            Expr::Subquery(subquery) => subquery,
            Expr::Binary { rhs, .. } => {
                let Expr::Subquery(subquery) = rhs.as_ref() else {
                    panic!("binary RHS is a subquery");
                };
                subquery
            }
            _ => panic!("root expression contains the expected subquery"),
        }
    }

    fn root_filter_subquery(document: &HirDocument) -> &SubqueryExpr {
        let query = document
            .query(root_query(document))
            .expect("root query exists");
        let hir::QueryBlockBody::Select {
            filter: Some(Expr::Subquery(subquery)),
            ..
        } = &query.blocks[0].body
        else {
            panic!("root filter is a subquery expression");
        };
        subquery
    }

    #[test]
    fn scalar_row_and_exists_destinations_install_matching_bindings() {
        for sql in [
            "SELECT (SELECT 1)",
            "SELECT (1, 2) = (SELECT 3, 4)",
            "SELECT EXISTS (SELECT 1)",
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let subquery = root_subquery(&document);
            let mut program = program();
            let prepared = prepare_subquery(&mut program, &document, subquery)
                .expect("subquery destination prepares");

            match (&prepared.destination, subquery) {
                (
                    QueryDestination::RowValueSubqueryResult {
                        result_reg_start,
                        num_regs,
                    },
                    SubqueryExpr::Scalar { .. } | SubqueryExpr::Row { .. },
                ) => assert_eq!(
                    program.subquery_binding(prepared.query),
                    Some(SubqueryBinding::RowValue {
                        start: *result_reg_start,
                        count: *num_regs,
                    })
                ),
                (
                    QueryDestination::ExistsSubqueryResult { result_reg },
                    SubqueryExpr::Exists(_),
                ) => assert_eq!(
                    program.subquery_binding(prepared.query),
                    Some(SubqueryBinding::Exists {
                        register: *result_reg,
                    })
                ),
                _ => panic!("subquery kind has its matching destination"),
            }
            assert!(!prepared.correlated);
        }
    }

    #[test]
    fn in_destination_uses_resolved_comparison_metadata() {
        let document = analyze_sql(
            Schema::new(),
            "SELECT ('a' COLLATE nocase, 2) IN (SELECT 'A', 3)",
        );
        let subquery = root_subquery(&document);
        let SubqueryExpr::In {
            comparison, query, ..
        } = subquery
        else {
            panic!("root is an IN subquery");
        };
        let expected_affinity = comparison
            .components
            .iter()
            .map(|component| component.affinity.aff_mask())
            .collect::<String>();
        let mut program = program();
        let prepared =
            prepare_subquery(&mut program, &document, subquery).expect("IN destination prepares");

        let QueryDestination::EphemeralIndex {
            cursor_id,
            index,
            affinity_str: Some(affinity_str),
            is_delete: false,
        } = &prepared.destination
        else {
            panic!("IN uses an ephemeral index");
        };
        assert_eq!(affinity_str.as_str(), expected_affinity);
        assert_eq!(index.columns.len(), comparison.components.len());
        for (column, component) in index.columns.iter().zip(&comparison.components) {
            assert_eq!(
                column.collation,
                component
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
            );
        }
        assert_eq!(
            program.subquery_binding(*query),
            Some(SubqueryBinding::InIndex { cursor: *cursor_id })
        );
    }

    #[test]
    fn correlation_comes_from_the_resolved_query_capture_set() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let document = analyze_sql(schema, "SELECT (SELECT value) FROM items");
        let subquery = root_subquery(&document);
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, subquery)
            .expect("correlated subquery destination prepares");

        assert!(prepared.correlated);
        assert!(!document
            .query(prepared.query)
            .expect("subquery exists")
            .captures
            .is_empty());
    }

    #[test]
    fn subquery_wrapper_keeps_legacy_initialization_and_body_order() {
        for sql in [
            "SELECT (1, 2) = (SELECT 3, 4)",
            "SELECT EXISTS (SELECT 1)",
            "SELECT 1 IN (SELECT 2)",
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let subquery = root_subquery(&document);
            let mut program = program();
            let prepared = prepare_subquery(&mut program, &document, subquery)
                .expect("subquery destination prepares");
            let marker = program.alloc_register();
            emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
                assert_eq!(query, prepared.query);
                assert!(std::ptr::eq(destination, &prepared.destination));
                program.emit_insn(Insn::Integer {
                    value: 99,
                    dest: marker,
                });
                Ok(())
            })
            .expect("subquery wrapper emits");

            assert!(matches!(
                program.insns.first(),
                Some((Insn::Once { .. }, _))
            ));
            let marker_position = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(insn, Insn::Integer { value: 99, dest } if *dest == marker)
                })
                .expect("body marker was emitted");
            match &prepared.destination {
                QueryDestination::RowValueSubqueryResult {
                    result_reg_start,
                    num_regs,
                } => {
                    assert!(matches!(
                        program.insns.get(1),
                        Some((Insn::BeginSubrtn { .. }, _))
                    ));
                    assert_eq!(marker_position, 2 + num_regs);
                    for (offset, register) in
                        (*result_reg_start..*result_reg_start + *num_regs).enumerate()
                    {
                        assert!(matches!(
                            program.insns.get(2 + offset),
                            Some((Insn::Null { dest, dest_end: None }, _)) if *dest == register
                        ));
                    }
                    assert!(matches!(
                        program.insns.get(marker_position + 1),
                        Some((Insn::Return { .. }, _))
                    ));
                }
                QueryDestination::ExistsSubqueryResult { result_reg } => {
                    assert!(matches!(
                        program.insns.as_slice(),
                        [
                            (Insn::Once { .. }, _),
                            (Insn::BeginSubrtn { .. }, _),
                            (Insn::Integer { value: 0, dest }, _),
                            (Insn::Integer { value: 99, .. }, _),
                            (Insn::Return { .. }, _),
                        ] if *dest == *result_reg
                    ));
                }
                QueryDestination::EphemeralIndex { cursor_id, .. } => {
                    assert!(matches!(
                        program.insns.as_slice(),
                        [
                            (Insn::Once { .. }, _),
                            (Insn::OpenEphemeral { cursor_id: opened, is_table: false }, _),
                            (Insn::Integer { value: 99, .. }, _),
                        ] if *opened == *cursor_id
                    ));
                }
                _ => panic!("expression subquery has a supported destination"),
            }
        }
    }

    #[test]
    fn correlated_subquery_wrapper_does_not_emit_once() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let document = analyze_sql(schema, "SELECT (SELECT value) FROM items");
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
            .expect("correlated subquery destination prepares");

        emit_prepared_subquery(&mut program, &prepared, |_, _, _| Ok(()))
            .expect("correlated wrapper emits");

        assert!(prepared.correlated);
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Once { .. })));
    }

    #[test]
    fn subquery_wrapper_preserves_scalar_and_list_eqp_nodes() {
        for (sql, list) in [
            ("SELECT (SELECT 1)", false),
            ("SELECT 1 IN (SELECT 2)", true),
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let mut program = program_with_mode(QueryMode::ExplainQueryPlan {
                format: turso_parser::ast::EqpFormat::Text,
            });
            let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
                .expect("subquery destination prepares");
            emit_prepared_subquery(&mut program, &prepared, |_, _, _| Ok(()))
                .expect("subquery wrapper emits");

            assert!(matches!(
                program.insns.first(),
                Some((
                    Insn::Explain { detail, .. },
                    _
                )) if matches!(
                    detail.as_ref(),
                    EqpDetail::ListSubquery { correlated: false, .. } if list
                ) || matches!(
                    detail.as_ref(),
                    EqpDetail::ScalarSubquery { correlated: false, .. } if !list
                )
            ));
        }
    }

    #[test]
    fn constant_select_binds_outputs_and_emits_one_result_row() {
        let document = analyze_sql(Schema::new(), "SELECT 1, 2");
        let query_id = root_query(&document);
        let query = document.query(query_id).expect("root query exists");
        let outputs = &query.blocks[0].outputs;
        let mut program = program();

        emit_query_body(
            &mut program,
            &document,
            query_id,
            &QueryDestination::ResultRows,
        )
        .expect("constant SELECT emits");

        let start = match program.insns.as_slice() {
            [(
                Insn::Integer {
                    value: 1,
                    dest: first,
                },
                _,
            ), (
                Insn::Integer {
                    value: 2,
                    dest: second,
                },
                _,
            ), (
                Insn::ResultRow {
                    start_reg,
                    count: 2,
                },
                _,
            )] if *second == *first + 1 && *start_reg == *first => *first,
            _ => panic!("constant SELECT keeps consecutive output registers"),
        };
        for (position, output) in outputs.iter().enumerate() {
            assert_eq!(
                program
                    .output_binding(output.id)
                    .expect("output register is bound")
                    .register,
                start + position
            );
        }
    }

    #[test]
    fn values_emits_every_row_without_hoisting_constants() {
        let document = analyze_sql(Schema::new(), "VALUES (1, 2), (3, 4)");
        let mut program = program();

        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("VALUES emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::ResultRow { count: 2, .. }))
                .count(),
            2
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter_map(|(insn, _)| match insn {
                    Insn::Integer { value, .. } => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn scalar_and_in_subqueries_emit_the_legacy_number_of_rows() {
        let scalar = analyze_sql(Schema::new(), "SELECT (VALUES (1), (2))");
        let mut scalar_program = program();
        let scalar_prepared =
            prepare_subquery(&mut scalar_program, &scalar, root_subquery(&scalar))
                .expect("scalar destination prepares");
        emit_prepared_subquery(
            &mut scalar_program,
            &scalar_prepared,
            |program, query, destination| emit_query_body(program, &scalar, query, destination),
        )
        .expect("scalar query emits");
        assert!(scalar_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Integer { value: 1, .. })));
        assert!(!scalar_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Integer { value: 2, .. })));
        assert_eq!(
            scalar_program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Copy { .. }))
                .count(),
            1
        );

        let membership = analyze_sql(Schema::new(), "SELECT 2 IN (VALUES (1), (2))");
        let mut membership_program = program();
        let membership_prepared = prepare_subquery(
            &mut membership_program,
            &membership,
            root_subquery(&membership),
        )
        .expect("IN destination prepares");
        emit_prepared_subquery(
            &mut membership_program,
            &membership_prepared,
            |program, query, destination| emit_query_body(program, &membership, query, destination),
        )
        .expect("IN query emits");
        assert_eq!(
            membership_program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IdxInsert { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn exists_skips_output_evaluation_and_arrays_are_decoded() {
        let exists = analyze_sql(Schema::new(), "SELECT EXISTS (SELECT random())");
        let mut exists_program = program();
        let prepared = prepare_subquery(&mut exists_program, &exists, root_subquery(&exists))
            .expect("EXISTS destination prepares");
        emit_prepared_subquery(
            &mut exists_program,
            &prepared,
            |program, query, destination| emit_query_body(program, &exists, query, destination),
        )
        .expect("EXISTS query emits");
        assert!(!exists_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Function { .. })));
        let QueryDestination::ExistsSubqueryResult { result_reg } = prepared.destination else {
            panic!("EXISTS uses its result destination");
        };
        assert!(exists_program.insns.iter().any(|(insn, _)| {
            matches!(insn, Insn::Integer { value: 1, dest } if *dest == result_reg)
        }));

        let array = analyze_sql(Schema::new(), "SELECT ARRAY[1, 2]");
        let mut array_program = program();
        emit_query_body(
            &mut array_program,
            &array,
            root_query(&array),
            &QueryDestination::ResultRows,
        )
        .expect("array SELECT emits");
        assert!(array_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::ArrayDecode { .. })));
    }

    #[test]
    fn no_from_filter_branches_around_result_emission() {
        let document = analyze_sql(Schema::new(), "SELECT 7 WHERE 0");
        let mut program = program();
        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("filtered SELECT emits");

        let filter_jump = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: true,
                        ..
                    }
                )
            })
            .expect("filter emits a false-or-null jump");
        let output = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Integer { value: 7, .. }))
            .expect("result expression is emitted");
        let result = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::ResultRow { .. }))
            .expect("result destination is emitted");
        assert!(filter_jump < output && output < result);
    }

    #[test]
    fn no_from_limit_and_offset_keep_row_counter_opcodes() {
        let document = analyze_sql(Schema::new(), "SELECT 1 LIMIT 1 OFFSET 1");
        let mut program = program();
        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("limited SELECT emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: false,
                        ..
                    }
                ))
                .count(),
            1
        );
        let offset_limit = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
            .expect("offset and limit are combined");
        let limit_exit = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: false,
                        ..
                    }
                )
            })
            .expect("limit exits before rows when its counter is false");
        assert!(offset_limit < limit_exit);
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IfPos { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::ResultRow { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::DecrJumpZero { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn scalar_select_offset_skips_its_only_candidate_row() {
        let document = analyze_sql(Schema::new(), "SELECT (SELECT 1 LIMIT 1 OFFSET 1)");
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
            .expect("scalar destination prepares");
        emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
            emit_query_body(program, &document, query, destination)
        })
        .expect("offset scalar query emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IfPos { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Copy { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Goto { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn floating_limit_and_offset_keep_integer_checks() {
        let document = analyze_sql(Schema::new(), "SELECT 1 LIMIT 1.0 OFFSET 0.0");
        let mut program = program();
        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("floating counters emit");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::MustBeInt { .. }))
                .count(),
            3
        );
    }

    #[test]
    fn planned_full_scan_binds_its_source_and_emits_residual_filter() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value INTEGER)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let plan = analyze_plan(
            schema,
            "SELECT value FROM items WHERE value LIMIT 2 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let source = plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0]
            .source;
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned full scan emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: None,
        } = program
            .source_binding(source)
            .copied()
            .expect("source has a physical binding")
        else {
            panic!("full table scan binds one B-tree cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let open = position(&|insn| {
            matches!(
                insn,
                Insn::OpenRead {
                    cursor_id,
                    root_page: 2,
                    db: MAIN_DB_ID,
                } if *cursor_id == scan_cursor
            )
        });
        let rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let filter = position(&|insn| {
            matches!(
                insn,
                Insn::IfNot {
                    jump_if_null: true,
                    ..
                }
            )
        });
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 1, .. }));
        let next = position(&|insn| {
            matches!(
                insn,
                Insn::Next {
                    cursor_id,
                    fullscan: true,
                    ..
                } if *cursor_id == scan_cursor
            )
        });
        assert!(open < rewind && rewind < filter && filter < result && result < next);
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IfPos { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::DecrJumpZero { .. })));
    }

    #[test]
    fn planned_derived_table_materializes_once_and_scans_its_hir_query() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT d.value \
             FROM (SELECT value FROM items NOT INDEXED WHERE value > 1) AS d \
             WHERE d.value < 5",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let [planned_loop] = block_plan.loops.as_slice() else {
            panic!("derived table has one physical loop");
        };
        let HirSourceAccess::Derived {
            query: derived_query,
        } = planned_loop.access
        else {
            panic!("derived source keeps its prepared HIR query");
        };
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("derived-table query emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: None,
        } = program
            .source_binding(planned_loop.source)
            .copied()
            .expect("derived source is bound")
        else {
            panic!("materialized derived source uses one table cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let once = position(&|insn| matches!(insn, Insn::Once { .. }));
        let open = position(
            &|insn| matches!(insn, Insn::OpenEphemeral { cursor_id, is_table: true } if *cursor_id == scan_cursor),
        );
        let insert =
            position(&|insn| matches!(insn, Insn::Insert { cursor, .. } if *cursor == scan_cursor));
        let rewind = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| {
                matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == scan_cursor)
                    .then_some(position)
            })
            .last()
            .expect("materialized table is scanned");
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 1, .. }));
        let next = position(
            &|insn| matches!(insn, Insn::Next { cursor_id, .. } if *cursor_id == scan_cursor),
        );

        assert_ne!(derived_query, query);
        assert!(once < open && open < insert && insert < rewind);
        assert!(rewind < result && result < next);
    }

    #[test]
    fn planned_single_reference_cte_materializes_its_hir_query() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "WITH chosen AS (\
                 SELECT value FROM items NOT INDEXED WHERE value > 1\
             ) \
             SELECT c.value FROM chosen AS c WHERE c.value < 5",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let [planned_loop] = block_plan.loops.as_slice() else {
            panic!("CTE reference has one physical loop");
        };
        let HirSourceAccess::Cte {
            query: cte_query,
            materialization,
            ..
        } = &planned_loop.access
        else {
            panic!("CTE source keeps its prepared HIR query");
        };
        assert_eq!(*materialization, HirCteMaterialization::PerReference);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("single-reference CTE query emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: None,
        } = program
            .source_binding(planned_loop.source)
            .copied()
            .expect("CTE source is bound")
        else {
            panic!("materialized CTE source uses one table cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let once = position(&|insn| matches!(insn, Insn::Once { .. }));
        let open = position(
            &|insn| matches!(insn, Insn::OpenEphemeral { cursor_id, is_table: true } if *cursor_id == scan_cursor),
        );
        let insert =
            position(&|insn| matches!(insn, Insn::Insert { cursor, .. } if *cursor == scan_cursor));
        let rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 1, .. }));

        assert_ne!(*cte_query, query);
        assert!(once < open && open < insert && insert < rewind && rewind < result);
    }

    #[test]
    fn planned_shared_cte_materializes_once_and_opens_one_cursor_per_reference() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "WITH chosen AS (\
                 SELECT value FROM items NOT INDEXED WHERE value > 1\
             ) \
             SELECT a.value, b.value \
             FROM chosen AS a JOIN chosen AS b ON b.value = a.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 2);
        let ctes = block_plan
            .loops
            .iter()
            .map(|planned_loop| {
                let HirSourceAccess::Cte {
                    cte,
                    materialization,
                    ..
                } = &planned_loop.access
                else {
                    panic!("both sources use the shared CTE");
                };
                assert_eq!(*materialization, HirCteMaterialization::Shared);
                *cte
            })
            .collect::<Vec<_>>();
        assert_eq!(ctes[0], ctes[1]);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("shared CTE query emits");

        let cursors = block_plan
            .loops
            .iter()
            .map(|planned_loop| {
                let SourceBinding::BTree {
                    scan_cursor,
                    table_cursor: None,
                } = program
                    .source_binding(planned_loop.source)
                    .copied()
                    .expect("shared CTE source is bound")
                else {
                    panic!("each CTE reference uses one table cursor");
                };
                scan_cursor
            })
            .collect::<Vec<_>>();
        assert_ne!(cursors[0], cursors[1]);
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OpenEphemeral { is_table: true, .. }))
                .count(),
            1
        );
        let duplicates = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::OpenDup {
                    new_cursor_id,
                    original_cursor_id,
                } => Some((*new_cursor_id, *original_cursor_id)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let [duplicate] = duplicates.as_slice() else {
            panic!("second CTE reference opens one duplicate cursor");
        };
        assert!(cursors.contains(&duplicate.0));
        assert!(cursors.contains(&duplicate.1));
        assert!(cursors.iter().all(|cursor| program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::Rewind { cursor_id, .. } if cursor_id == cursor)
        )));
        assert!(program.hir_materialized_cte(ctes[0]).is_some());
    }

    #[test]
    fn planned_explicit_cte_uses_registered_materialized_storage() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "WITH chosen AS MATERIALIZED (\
                 SELECT value FROM items NOT INDEXED\
             ) \
             SELECT value FROM chosen",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        let HirSourceAccess::Cte {
            cte,
            materialization,
            ..
        } = &planned_loop.access
        else {
            panic!("explicit CTE keeps its CTE access");
        };
        assert_eq!(*materialization, HirCteMaterialization::Explicit);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("explicit CTE query emits");

        assert!(program.hir_materialized_cte(*cte).is_some());
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OpenEphemeral { is_table: true, .. }))
                .count(),
            1
        );
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenDup { .. })));
    }

    #[cfg(feature = "json")]
    #[test]
    fn planned_virtual_table_uses_resolved_arguments_and_virtual_cursor() {
        let plan = analyze_plan(Schema::new(), "SELECT value FROM json_each('[1]')");
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        let HirSourceAccess::Virtual(operation) = &planned_loop.access else {
            panic!("table function uses virtual-table access");
        };
        assert!(matches!(
            operation.arguments.as_slice(),
            [Expr::Literal(Literal::String(_))]
        ));
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("virtual-table query emits");

        let SourceBinding::Virtual { cursor } = program
            .source_binding(planned_loop.source)
            .copied()
            .expect("virtual source is bound")
        else {
            panic!("virtual source uses one virtual cursor");
        };
        assert!(matches!(
            program.get_cursor_type(cursor),
            Some(CursorType::VirtualTable(_))
        ));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::VOpen { cursor_id } if *cursor_id == cursor)));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::VFilter {
                cursor_id,
                arg_count: 1,
                idx_num,
                ..
            } if *cursor_id == cursor && *idx_num == operation.idx_num as usize
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::VColumn { cursor_id, .. } if *cursor_id == cursor
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::VNext { cursor_id, .. } if *cursor_id == cursor
        )));
    }

    #[test]
    fn planned_cross_join_nests_loops_and_places_predicates_when_ready() {
        let plan = analyze_plan(
            joined_items_schema(),
            "SELECT l.value, r.value \
             FROM left_items AS l NOT INDEXED CROSS JOIN right_items AS r NOT INDEXED \
             WHERE l.value AND r.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 2);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned cross join emits");

        let cursors = block_plan
            .loops
            .iter()
            .map(|planned_loop| {
                let SourceBinding::BTree {
                    scan_cursor,
                    table_cursor: None,
                } = program
                    .source_binding(planned_loop.source)
                    .copied()
                    .expect("join source is bound")
                else {
                    panic!("table scan uses one cursor");
                };
                scan_cursor
            })
            .collect::<Vec<_>>();
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let outer_rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == cursors[0]),
        );
        let outer_column = position(
            &|insn| matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == cursors[0]),
        );
        let inner_rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == cursors[1]),
        );
        let inner_column = position(
            &|insn| matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == cursors[1]),
        );
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 2, .. }));
        let advances = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::Next {
                    cursor_id,
                    fullscan: true,
                    ..
                } => Some(*cursor_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let empty_targets = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::Rewind {
                    cursor_id,
                    pc_if_empty,
                } if cursors.contains(cursor_id) => Some(*pc_if_empty),
                _ => None,
            })
            .collect::<Vec<_>>();
        let predicate_failures = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::IfNot {
                    target_pc,
                    jump_if_null: true,
                    ..
                } => Some(*target_pc),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(outer_rewind < outer_column && outer_column < inner_rewind);
        assert!(inner_rewind < inner_column && inner_column < result);
        assert_eq!(advances, vec![cursors[1], cursors[0]]);
        assert_eq!(empty_targets.len(), 2);
        assert_eq!(predicate_failures.len(), 2);
        for (empty, predicate_failure) in empty_targets.iter().zip(&predicate_failures) {
            assert_ne!(empty, predicate_failure);
        }
    }

    #[test]
    fn planned_inner_join_seek_reads_its_outer_hir_source() {
        for (output, table_required) in [("r.value", false), ("r.extra", true)] {
            let sql = format!(
                "SELECT l.value, {output} \
                 FROM left_items AS l NOT INDEXED \
                 JOIN right_items AS r INDEXED BY right_items_value \
                 ON r.value = l.value"
            );
            let plan = analyze_plan(joined_items_schema(), &sql);
            let query = root_query(&plan.document);
            let block_plan = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0];
            assert_eq!(block_plan.loops.len(), 2);
            let HirSourceAccess::BTree {
                operation:
                    HirBtreeOperation::Seek {
                        index: Some(index), ..
                    },
                table_lookup,
            } = &block_plan.loops[1].access
            else {
                panic!("right join source uses its requested index seek");
            };
            assert_eq!(index.name, "right_items_value");
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned inner seek emits");

            let outer_cursor = match program
                .source_binding(block_plan.loops[0].source)
                .copied()
                .expect("outer source is bound")
            {
                SourceBinding::BTree { scan_cursor, .. } => scan_cursor,
                _ => panic!("outer source is a B-tree"),
            };
            let outer_key = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == outer_cursor)
                })
                .expect("seek reads its outer HIR column");
            let seek = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(
                        insn,
                        Insn::SeekGE { is_index: true, .. }
                            | Insn::SeekGT { is_index: true, .. }
                            | Insn::SeekLE { is_index: true, .. }
                            | Insn::SeekLT { is_index: true, .. }
                    )
                })
                .expect("inner source emits an index seek");
            assert!(outer_key < seek);
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. }))
                    .count(),
                usize::from(table_required)
            );
        }
    }

    #[test]
    fn planned_inner_join_builds_and_probes_automatic_index() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             CROSS JOIN right_items AS r \
             WHERE r.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("right join source uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("automatic-index join emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor,
        } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("automatic-index source is bound")
        else {
            panic!("automatic-index source has a B-tree binding");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let open = position(
            &|insn| matches!(insn, Insn::OpenAutoindex { cursor_id } if *cursor_id == scan_cursor),
        );
        let insert = position(
            &|insn| matches!(insn, Insn::IdxInsert { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let filter = position(
            &|insn| matches!(insn, Insn::Filter { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let seek = position(&|insn| {
            matches!(
                insn,
                Insn::SeekGE {
                    cursor_id,
                    is_index: true,
                    ..
                } if *cursor_id == scan_cursor
            )
        });

        assert!(open < insert && insert < filter && filter < seek);
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::FilterAdd { cursor_id, .. } if *cursor_id == scan_cursor)
        ));
        assert!(!program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::OpenRead { cursor_id, .. } if *cursor_id == scan_cursor)
        ));
        assert!(
            table_cursor.is_none(),
            "payload makes the automatic index covering"
        );
    }

    #[test]
    fn null_matching_automatic_index_does_not_use_bloom_filter() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT r.extra \
             FROM left_items AS l NOT INDEXED \
             CROSS JOIN right_items AS r \
             WHERE r.value IS l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("right join source uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("NULL-matching automatic-index join emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("automatic-index source is bound")
        else {
            panic!("automatic-index source has a B-tree binding");
        };
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::OpenAutoindex { cursor_id } if *cursor_id == scan_cursor)
        ));
        assert!(!program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::FilterAdd { cursor_id, .. } | Insn::Filter { cursor_id, .. }
                if *cursor_id == scan_cursor
        )));
    }

    #[test]
    fn automatic_index_build_lowers_virtual_generated_column_from_base_table() {
        let plan = analyze_plan(
            automatic_generated_join_schema(),
            "SELECT r.extra \
             FROM left_items AS l NOT INDEXED \
             CROSS JOIN right_items AS r \
             WHERE r.generated = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("generated join key uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        assert_eq!(index.columns[0].pos_in_table, 1);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("generated automatic-index join emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("automatic-index source is bound")
        else {
            panic!("automatic-index source has a B-tree binding");
        };
        let add = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Add { .. }))
            .expect("generated expression is lowered while building the index");
        let insert = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(insn, Insn::IdxInsert { cursor_id, .. } if *cursor_id == scan_cursor)
            })
            .expect("automatic index is populated");
        assert!(add < insert);
    }

    #[test]
    fn left_join_checks_on_before_marking_match_and_where_afterward() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r NOT INDEXED ON r.value = l.value \
             WHERE r.extra IS NULL",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let right_loop = &block_plan.loops[1];
        assert_eq!(block_plan.predicates.len(), 2);
        assert_eq!(
            block_plan.predicates[0].from_outer_join,
            Some(right_loop.source)
        );
        assert_eq!(block_plan.predicates[1].from_outer_join, None);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("LEFT JOIN emits");

        let SourceBinding::BTree {
            scan_cursor: right_cursor,
            table_cursor: None,
        } = program
            .source_binding(right_loop.source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("NOT INDEXED right source uses one B-tree cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let reset = position(&|insn| matches!(insn, Insn::Integer { value: 0, .. }));
        let match_register = match program.insns[reset].0 {
            Insn::Integer { dest, .. } => dest,
            _ => unreachable!(),
        };
        let rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == right_cursor),
        );
        let matched = position(
            &|insn| matches!(insn, Insn::Integer { value: 1, dest } if *dest == match_register),
        );
        let failures = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| {
                matches!(insn, Insn::IfNot { .. }).then_some(position)
            })
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 2);
        let [on_failure, where_failure] = failures.as_slice() else {
            unreachable!()
        };
        let result = position(&|insn| matches!(insn, Insn::ResultRow { .. }));
        let next = position(
            &|insn| matches!(insn, Insn::Next { cursor_id, .. } if *cursor_id == right_cursor),
        );
        let check =
            position(&|insn| matches!(insn, Insn::IfPos { reg, .. } if *reg == match_register));
        let null_row = position(
            &|insn| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == right_cursor),
        );
        let retry = program
            .insns
            .iter()
            .enumerate()
            .skip(null_row + 1)
            .find_map(|(position, (insn, _))| matches!(insn, Insn::Goto { .. }).then_some(position))
            .expect("NULL row re-enters after ON predicates");

        assert!(reset < rewind && rewind < *on_failure && *on_failure < matched);
        assert!(matched < *where_failure && *where_failure < result && result < next);
        assert!(next < check && check < null_row && null_row < retry);
    }

    #[test]
    fn left_join_constant_on_runs_at_the_right_loop_boundary() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r NOT INDEXED ON 0",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(
            block_plan.predicates[0].from_outer_join,
            Some(block_plan.loops[1].source)
        );
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("constant-ON LEFT JOIN emits");

        let reset = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Integer { value: 0, .. }))
            .expect("match flag is reset");
        let match_register = match program.insns[reset].0 {
            Insn::Integer { dest, .. } => dest,
            _ => unreachable!(),
        };
        let on_failure = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::IfNot { .. }))
            .expect("constant ON predicate is checked");
        let matched = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(insn, Insn::Integer { value: 1, dest } if *dest == match_register)
            })
            .expect("match flag is set after ON");
        assert!(reset < on_failure && on_failure < matched);
    }

    #[test]
    fn left_join_uses_automatic_index_and_null_extends_its_scan_cursor() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r ON r.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("right source uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("automatic-index LEFT JOIN emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("right source has a B-tree binding");
        };
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::OpenAutoindex { cursor_id } if *cursor_id == scan_cursor)
        ));
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == scan_cursor)
        ));
    }

    #[test]
    fn left_join_null_extends_index_and_table_cursors() {
        let plan = analyze_plan(
            joined_items_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r INDEXED BY right_items_value \
                 ON r.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("indexed LEFT JOIN emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: Some(table_cursor),
        } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("uncovered output uses index and table cursors");
        };
        for cursor in [scan_cursor, table_cursor] {
            assert!(program.insns.iter().any(
                |(insn, _)| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == cursor)
            ));
        }
    }

    #[test]
    fn unmatched_left_join_row_restarts_downstream_loop() {
        let plan = analyze_plan(
            left_join_with_tail_schema(),
            "SELECT l.value, r.extra, t.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r NOT INDEXED ON r.value = l.value \
             CROSS JOIN tail_items AS t NOT INDEXED \
             WHERE t.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 3);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("LEFT JOIN with downstream loop emits");

        let cursor = |source| match program
            .source_binding(source)
            .copied()
            .expect("source is bound")
        {
            SourceBinding::BTree { scan_cursor, .. } => scan_cursor,
            _ => panic!("source uses a B-tree cursor"),
        };
        let right_cursor = cursor(block_plan.loops[1].source);
        let tail_cursor = cursor(block_plan.loops[2].source);
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let matched = position(&|insn| matches!(insn, Insn::Integer { value: 1, .. }));
        let tail_rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == tail_cursor),
        );
        let tail_next = position(
            &|insn| matches!(insn, Insn::Next { cursor_id, .. } if *cursor_id == tail_cursor),
        );
        let null_row = position(
            &|insn| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == right_cursor),
        );
        let retry = program
            .insns
            .iter()
            .enumerate()
            .skip(null_row + 1)
            .find_map(|(position, (insn, _))| matches!(insn, Insn::Goto { .. }).then_some(position))
            .expect("NULL row re-enters the downstream loop");

        assert!(matched < tail_rewind && tail_rewind < tail_next);
        assert!(tail_next < null_row && null_row < retry);
    }

    #[test]
    fn parenthesized_left_join_marks_and_null_extends_the_whole_group() {
        let plan = analyze_plan(
            left_join_with_tail_schema(),
            "SELECT l.value, r.extra, t.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN (\
                 right_items AS r NOT INDEXED \
                 JOIN tail_items AS t NOT INDEXED ON t.value = r.value\
             ) ON r.value = l.value \
             WHERE r.extra IS NULL",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 3);
        let [group] = block_plan.groups.as_slice() else {
            panic!("one parenthesized FROM group is planned");
        };
        let group_loop_indices = block_plan
            .loops
            .iter()
            .enumerate()
            .filter_map(|(index, planned)| {
                group
                    .source_range
                    .contains(&planned.source_position)
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(group_loop_indices.len(), 2);
        assert_eq!(group_loop_indices[1], group_loop_indices[0] + 1);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("parenthesized LEFT JOIN emits");

        let group_cursors = group_loop_indices
            .iter()
            .map(|&index| {
                let source = block_plan.loops[index].source;
                let SourceBinding::BTree { scan_cursor, .. } = program
                    .source_binding(source)
                    .copied()
                    .expect("group source is bound")
                else {
                    panic!("group source uses a B-tree cursor");
                };
                scan_cursor
            })
            .collect::<Vec<_>>();
        let reset = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Integer { value: 0, .. }))
            .expect("group match flag is reset");
        let match_register = match program.insns[reset].0 {
            Insn::Integer { dest, .. } => dest,
            _ => unreachable!(),
        };
        let matched = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(insn, Insn::Integer { value: 1, dest } if *dest == match_register)
            })
            .expect("group match flag is set");
        let failures = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| {
                matches!(insn, Insn::IfNot { .. }).then_some(position)
            })
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 3);
        assert!(failures[..2].iter().all(|failure| *failure < matched));
        assert!(matched < failures[2]);

        let last_group_next = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| match insn {
                Insn::Next { cursor_id, .. } if group_cursors.contains(cursor_id) => Some(position),
                _ => None,
            })
            .max()
            .expect("both group loops advance");
        let null_rows = group_cursors
            .iter()
            .map(|cursor| {
                program
                    .insns
                    .iter()
                    .position(|(insn, _)| {
                        matches!(insn, Insn::NullRow { cursor_id } if cursor_id == cursor)
                    })
                    .expect("each group cursor is null-extended")
            })
            .collect::<Vec<_>>();
        assert!(null_rows.iter().all(|null_row| last_group_next < *null_row));
    }

    #[test]
    fn planned_index_scan_opens_table_only_for_uncovered_columns() {
        for (sql, table_required) in [
            ("SELECT value FROM items INDEXED BY items_value", false),
            ("SELECT extra FROM items INDEXED BY items_value", true),
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            let HirSourceAccess::BTree {
                operation:
                    HirBtreeOperation::Scan {
                        index: Some(index), ..
                    },
                table_lookup,
            } = &planned_loop.access
            else {
                let HirSourceAccess::BTree { operation, .. } = &planned_loop.access else {
                    panic!("INDEXED BY selects B-tree access for {sql}");
                };
                panic!("INDEXED BY selects an index scan for {sql}: {operation:?}");
            };
            assert_eq!(index.name, "items_value");
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let source = planned_loop.source;
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned index scan emits");

            let SourceBinding::BTree {
                scan_cursor,
                table_cursor,
            } = program
                .source_binding(source)
                .copied()
                .expect("source has a physical binding")
            else {
                panic!("index scan binds B-tree cursors");
            };
            assert!(matches!(
                program.get_cursor_type(scan_cursor),
                Some(CursorType::BTreeIndex(index)) if index.name == "items_value"
            ));
            assert_eq!(table_cursor.is_some(), table_required);
            assert!(program.insns.iter().any(|(insn, _)| {
                matches!(
                    insn,
                    Insn::OpenRead {
                        cursor_id,
                        root_page: 3,
                        db: MAIN_DB_ID,
                    } if *cursor_id == scan_cursor
                )
            }));

            match table_cursor {
                Some(table_cursor) => {
                    assert!(program.insns.iter().any(|(insn, _)| {
                        matches!(
                            insn,
                            Insn::DeferredSeek {
                                index_cursor_id,
                                table_cursor_id,
                            } if *index_cursor_id == scan_cursor
                                && *table_cursor_id == table_cursor
                        )
                    }));
                    assert!(program.insns.iter().any(|(insn, _)| {
                        matches!(
                            insn,
                            Insn::Column {
                                cursor_id,
                                column: 1,
                                ..
                            } if *cursor_id == table_cursor
                        )
                    }));
                }
                None => {
                    assert!(!program
                        .insns
                        .iter()
                        .any(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. })));
                    assert!(program.insns.iter().any(|(insn, _)| {
                        matches!(
                            insn,
                            Insn::Column {
                                cursor_id,
                                column: 0,
                                ..
                            } if *cursor_id == scan_cursor
                        )
                    }));
                }
            }
        }
    }

    #[test]
    fn not_indexed_keeps_the_hir_plan_on_the_table() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value FROM items NOT INDEXED",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];

        assert!(matches!(
            &planned_loop.access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::Scan { index: None, .. },
                table_lookup: BtreeTableLookup::ScanOnly,
            }
        ));
    }

    #[test]
    fn planned_rowid_equality_emits_one_point_lookup() {
        for sql in [
            "SELECT value FROM items WHERE rowid = 2",
            "SELECT value FROM items WHERE id = 2",
        ] {
            let plan = analyze_plan(rowid_items_schema(), sql);
            let query = root_query(&plan.document);
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            assert!(matches!(
                &planned_loop.access,
                HirSourceAccess::BTree {
                    operation: HirBtreeOperation::RowidEq { .. },
                    table_lookup: BtreeTableLookup::ScanOnly,
                }
            ));
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned rowid equality emits");

            let SourceBinding::BTree {
                scan_cursor,
                table_cursor: None,
            } = program
                .source_binding(planned_loop.source)
                .copied()
                .expect("source has a physical binding")
            else {
                panic!("rowid equality binds one table cursor");
            };
            assert!(matches!(
                program.get_cursor_type(scan_cursor),
                Some(CursorType::BTreeTable(table)) if table.name == "items"
            ));
            assert!(program.insns.iter().any(|(insn, _)| {
                matches!(
                    insn,
                    Insn::SeekRowid { cursor_id, .. } if *cursor_id == scan_cursor
                )
            }));
            assert!(!program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::Next { .. } | Insn::Prev { .. })));
        }
    }

    #[test]
    fn planned_hir_ranges_use_the_shared_seek_emitter() {
        for (sql, table_required) in [
            (
                "SELECT value FROM items INDEXED BY items_value WHERE value >= 2 AND value < 5",
                false,
            ),
            (
                "SELECT extra FROM items INDEXED BY items_value WHERE value >= 2 AND value < 5",
                true,
            ),
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            let HirSourceAccess::BTree {
                operation:
                    HirBtreeOperation::Seek {
                        index: Some(index), ..
                    },
                table_lookup,
            } = &planned_loop.access
            else {
                panic!("indexed range selects an index seek: {sql}");
            };
            assert_eq!(index.name, "items_value");
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned index range emits");

            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::SeekGE { is_index: true, .. }
                    | Insn::SeekGT { is_index: true, .. }
                    | Insn::SeekLE { is_index: true, .. }
                    | Insn::SeekLT { is_index: true, .. }
            )));
            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::IdxGE { .. } | Insn::IdxGT { .. } | Insn::IdxLE { .. } | Insn::IdxLT { .. }
            )));
            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::Next {
                    fullscan: false,
                    ..
                } | Insn::Prev {
                    fullscan: false,
                    ..
                }
            )));
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. }))
                    .count(),
                usize::from(table_required)
            );
        }

        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT value FROM items WHERE rowid >= 2 AND rowid < 5",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        assert!(matches!(
            &planned_loop.access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::Seek { index: None, .. },
                table_lookup: BtreeTableLookup::ScanOnly,
            }
        ));
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned rowid range emits");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::SeekGE {
                is_index: false,
                ..
            } | Insn::SeekGT {
                is_index: false,
                ..
            } | Insn::SeekLE {
                is_index: false,
                ..
            } | Insn::SeekLT {
                is_index: false,
                ..
            }
        )));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::RowId { .. })));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Ge { .. } | Insn::Gt { .. } | Insn::Le { .. } | Insn::Lt { .. }
        )));
    }

    #[test]
    fn planned_hir_in_lists_use_the_shared_two_level_loop() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT value FROM items WHERE rowid IN (1, 2, 2)",
        );
        let query = root_query(&plan.document);
        assert!(matches!(
            &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0]
                .access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::InSeek {
                    index: None,
                    source: HirInSeekSource::Values { values, .. },
                },
                ..
            } if values.len() == 3
        ));
        let mut rowid_program = program();

        emit_planned_query_body(
            &mut rowid_program,
            &plan,
            query,
            &QueryDestination::ResultRows,
        )
        .expect("planned rowid IN seek emits");

        assert_eq!(
            rowid_program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IdxInsert { .. }))
                .count(),
            3
        );
        assert!(rowid_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SeekRowid { .. })));
        assert!(!rowid_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IdxGT { .. })));

        for (sql, table_required) in [
            (
                "SELECT value FROM items INDEXED BY items_value WHERE value IN (1, 2, 2)",
                false,
            ),
            (
                "SELECT extra FROM items INDEXED BY items_value WHERE value IN (1, 2, 2)",
                true,
            ),
        ] {
            let mut plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let values = match &plan
                .document
                .query(query)
                .expect("root query exists")
                .blocks[0]
                .body
            {
                hir::QueryBlockBody::Select {
                    filter: Some(Expr::InList { values, .. }),
                    ..
                } => values.clone(),
                _ => panic!("indexed test has an IN-list filter"),
            };
            let planned_query = plan
                .queries
                .iter_mut()
                .find(|planned| planned.query == query)
                .expect("root query is planned");
            let HirSourceAccess::BTree { operation, .. } =
                &mut planned_query.blocks[0].loops[0].access
            else {
                panic!("indexed IN uses a B-tree plan");
            };
            let HirBtreeOperation::Scan {
                index: Some(index), ..
            } = operation
            else {
                panic!("cost model starts from the requested index");
            };
            *operation = HirBtreeOperation::InSeek {
                index: Some(index.clone()),
                source: HirInSeekSource::Values {
                    values,
                    affinity: Affinity::Integer,
                },
            };
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            let HirSourceAccess::BTree {
                operation,
                table_lookup,
            } = &planned_loop.access
            else {
                panic!("indexed IN uses a B-tree plan");
            };
            assert!(
                matches!(
                    operation,
                    HirBtreeOperation::InSeek {
                        index: Some(index),
                        source: HirInSeekSource::Values { .. },
                    } if index.name == "items_value"
                ),
                "unexpected indexed IN operation for {sql}: {operation:?}"
            );
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned indexed IN seek emits");

            assert!(program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::SeekGE { is_index: true, .. })));
            assert!(program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::IdxGT { .. })));
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. }))
                    .count(),
                usize::from(table_required)
            );
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(
                        insn,
                        Insn::Next {
                            fullscan: false,
                            ..
                        }
                    ))
                    .count(),
                2
            );
        }
    }

    #[test]
    fn planned_hir_in_subquery_reuses_its_prepared_index() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT value FROM items WHERE rowid IN (VALUES (1), (2))",
        );
        let query = root_query(&plan.document);
        let subquery = root_filter_subquery(&plan.document);
        let SubqueryExpr::In {
            query: source_query,
            ..
        } = subquery
        else {
            panic!("root filter is an IN subquery");
        };
        assert!(matches!(
            &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0]
                .access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::InSeek {
                    source: HirInSeekSource::Query { query },
                    ..
                },
                ..
            } if query == source_query
        ));
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &plan.document, subquery)
            .expect("IN subquery destination prepares");
        let Some(SubqueryBinding::InIndex {
            cursor: source_cursor,
        }) = program.subquery_binding(*source_query)
        else {
            panic!("IN subquery has an index binding");
        };
        emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
            emit_planned_query_body(program, &plan, query, destination)
        })
        .expect("IN subquery emits");

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned query IN seek emits");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Rewind { cursor_id, .. } if *cursor_id == source_cursor
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Column {
                cursor_id,
                column: 0,
                ..
            } if *cursor_id == source_cursor
        )));
    }

    #[test]
    fn shared_hir_seek_emitter_keeps_backward_bounds() {
        use crate::{
            translate::plan::{SeekDef, SeekKey, SeekKeyComponent},
            types::SeekOp,
        };

        let document = analyze_sql(rowid_items_schema(), "SELECT value FROM items");
        let source = &document.sources[0];
        let seek_def = SeekDef {
            prefix: Vec::new(),
            start: SeekKey {
                last_component: SeekKeyComponent::Expr(Expr::Literal(Literal::Numeric(
                    "5".to_string(),
                ))),
                op: SeekOp::LE { eq_only: false },
                affinity: Affinity::Integer,
            },
            end: SeekKey {
                last_component: SeekKeyComponent::Expr(Expr::Literal(Literal::Numeric(
                    "2".to_string(),
                ))),
                op: SeekOp::LE { eq_only: false },
                affinity: Affinity::Integer,
            },
            iter_dir: IterationDirection::Backwards,
        };
        let mut program = program();
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(match &source.kind {
            hir::SourceKind::Table(table) => match table.value() {
                Table::BTree(table) => table.clone(),
                _ => panic!("test source is a B-tree table"),
            },
            _ => panic!("test source is a table"),
        }));
        let start_register = program.alloc_register();
        let loop_start = program.allocate_label();
        let done = program.allocate_label();

        SeekEmitter::with_lowering(
            &mut program,
            &seek_def,
            HirSeekExpressionLowering {
                document: &document,
                source,
            },
            cursor,
            start_register,
            done,
            None,
        )
        .emit(loop_start, false)
        .expect("backward HIR seek emits");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::SeekLE {
                is_index: false,
                cursor_id,
                ..
            } if *cursor_id == cursor
        )));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Le { .. })));
    }
}
