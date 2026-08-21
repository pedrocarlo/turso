//! Runtime destination preparation for resolved HIR queries.

use turso_parser::ast::{Literal, SortOrder};

use crate::{
    schema::{Index, IndexColumn, Table},
    sync::Arc,
    translate::{
        eqp::EqpDetail,
        expr::ConditionMetadata,
        optimizer::HirBtreeOperation,
        plan::{IterationDirection, QueryDestination},
        result_row::emit_columns_to_destination,
        semantic::hir::{self, HirDocument, QueryId, SubqueryExpr},
        semantic_to_plan::{HirPlan, HirQueryBlockPlan, HirSourceAccess},
    },
    util::parse_numeric_literal,
    vdbe::{
        builder::{CursorType, ProgramBuilder, SourceBinding, SubqueryBinding},
        insn::Insn,
        BranchOffset,
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
    emit_single_btree_scan(
        program,
        document,
        query.limit.as_ref(),
        block,
        block_plan,
        destination,
    )
}

fn emit_single_btree_scan(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query_limit: Option<&hir::Limit>,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    destination: &QueryDestination,
) -> Result<()> {
    if block.aggregate_count != 0 || block.window_function_count != 0 || !block.windows.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} is not a supported full scan",
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
            "HIR query block {:?} has unsupported full-scan clauses",
            block.id
        )));
    };
    let [planned_loop] = block_plan.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} does not have one source loop",
            block.id
        )));
    };
    let HirSourceAccess::BTree(HirBtreeOperation::Scan {
        iter_dir,
        index: None,
    }) = &planned_loop.access
    else {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} does not use a full table scan",
            block.id
        )));
    };
    let source = document.source(planned_loop.source).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR full scan references missing source {}",
            planned_loop.source
        ))
    })?;
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(format!(
            "HIR full scan source {} is not a table",
            source.id
        )));
    };
    let Table::BTree(table) = table.value() else {
        return Err(LimboError::InternalError(format!(
            "HIR full scan source {} is not a B-tree table",
            source.id
        )));
    };
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR full scan source {} has no database",
            source.id
        ))
    })?;
    let database_snapshot = document.database(database).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR full scan source {} references missing database {database:?}",
            source.id
        ))
    })?;

    let done = program.allocate_label();
    let next = program.allocate_label();
    let loop_start = program.allocate_label();
    let limit = initialize_limit(program, document, query_limit, done)?;
    program.begin_read_on_database(database.index(), database_snapshot.schema_version)?;
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.bind_source(
        source.id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor: None,
        },
    );
    program.emit_insn(Insn::OpenRead {
        cursor_id: cursor,
        root_page: table.root_page,
        db: database.index(),
    });
    match iter_dir {
        IterationDirection::Forwards => program.emit_insn(Insn::Rewind {
            cursor_id: cursor,
            pc_if_empty: done,
        }),
        IterationDirection::Backwards => program.emit_insn(Insn::Last {
            cursor_id: cursor,
            pc_if_empty: done,
        }),
    }
    program.preassign_label_to_next_insn(loop_start);

    for predicate in block_plan.predicates.iter().filter(|term| !term.consumed) {
        if predicate.from_outer_join.is_some() {
            return Err(LimboError::InternalError(format!(
                "HIR query block {:?} full scan contains an outer-join predicate",
                block.id
            )));
        }
        let passed = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            &predicate.expr,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: passed,
                jump_target_when_false: next,
                jump_target_when_null: next,
            },
        )?;
        program.preassign_label_to_next_insn(passed);
    }

    emit_before_row(program, limit, next);
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
    program.preassign_label_to_next_insn(next);
    match iter_dir {
        IterationDirection::Forwards => program.emit_insn(Insn::Next {
            cursor_id: cursor,
            pc_if_next: loop_start,
            fullscan: true,
        }),
        IterationDirection::Backwards => program.emit_insn(Insn::Prev {
            cursor_id: cursor,
            pc_if_prev: loop_start,
            fullscan: true,
        }),
    }
    program.preassign_label_to_next_insn(done);
    Ok(())
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
}
