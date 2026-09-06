//! Window-buffer planning from resolved HIR.

use std::ops::ControlFlow;

use smallvec::SmallVec;

use crate::{
    function::{AccumulatorFunc, Func, WindowFunc},
    schema::{BTreeCharacteristics, BTreeTable, ColDef, Column, Type},
    sync::Arc,
    translate::{
        aggregation::hir_aggregate_function,
        collate::CollationSeq,
        order_by::custom_type_comparator_from_type_fact,
        semantic::hir::{self, ExprVisitor},
        semantic_lowering::expr::{
            translate_expr, translate_expr_no_constant_opt, translate_expr_with_inputs,
            ExprRegisterInput,
        },
        window::{
            emit_function_inverse_runtime, emit_function_step_runtime, emit_window_cursor_keys,
            emit_window_frame_offset, emit_window_key_gather, emit_window_partition_change,
            emit_window_partition_reset, emit_window_peer_change, open_window_buffer,
            prepare_window_frame_offsets, prepare_window_frame_tracking,
            prepare_window_input_state, prepare_window_peer_state, window_function_uses_subtypes,
            BufferedWindowValue, WindowBufferInput, WindowBufferRuntime, WindowFrameBoundSide,
            WindowFrameEdge, WindowFrameOffsets, WindowFrameShape, WindowFrameTracking,
            WindowFunctionRuntime, WindowFunctionRuntimeSpec, WindowInputState,
            WindowPartitionState, WindowPeerComparison, WindowPeerState, WindowStepContext,
            WindowValueEmitter,
        },
    },
    types::KeyInfo,
    vdbe::{builder::ProgramBuilder, insn::Insn, BranchOffset, CursorID},
    LimboError, Result,
};

use crate::translate::window::{
    emit_window_aggregate_results, emit_window_first_row_frame, emit_window_peer_seed,
    emit_window_subsequent_row, WindowAggregateResultMode, WindowEmptyFrameOutput,
    WindowFirstRowFrame,
};

#[cfg(test)]
use crate::translate::window::{
    emit_window_empty_frame_guard, emit_window_nonempty_branch, window_frame_order_check,
    WindowEmptyFrameState, WindowFrameOrderCheck,
};

#[cfg(test)]
use crate::translate::window::{
    emit_window_following_start_delay, emit_window_frame_cursor_rewind, WindowCursors,
};

/// One value stored for a window layer.
///
/// `Source` keeps the common direct-column case explicit. `Evaluate` also
/// represents rowid, aggregate, and subquery leaves needed when a subtype-
/// producing argument must be recomputed after the row leaves the buffer.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy)]
pub(super) enum HirWindowBufferColumn<'a> {
    Evaluate(&'a hir::Expr),
    Source {
        expression: &'a hir::Expr,
        reference: hir::ColumnRef,
    },
}

impl<'a> HirWindowBufferColumn<'a> {
    fn expression(&self) -> &'a hir::Expr {
        match self {
            Self::Evaluate(expression) | Self::Source { expression, .. } => expression,
        }
    }
}

/// One exact HIR leaf and the window-buffer column holding its value.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy)]
struct HirWindowLateInput<'a> {
    owner: &'a hir::Expr,
    expression: &'a hir::Expr,
    column: usize,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
pub(super) struct HirWindowFunction<'a> {
    pub(super) id: hir::WindowFunctionId,
    pub(super) function: AccumulatorFunc,
    pub(super) arguments: Vec<BufferedWindowValue<&'a hir::Expr>>,
    pub(super) filter: Option<BufferedWindowValue<&'a hir::Expr>>,
    pub(super) argument_facts: &'a [hir::FunctionArgumentFacts],
}

/// HIR-owned input layout for one effective window.
///
/// Columns are ordered like the legacy source subquery: PARTITION BY and
/// ORDER BY values first, followed by function arguments and FILTER values.
/// Equivalent partition/order and late-evaluation leaves share one slot.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
pub(super) struct HirWindowBufferPlan<'a> {
    pub(super) window: &'a hir::ResolvedWindow,
    pub(super) columns: Vec<HirWindowBufferColumn<'a>>,
    pub(super) partition_slots: Vec<usize>,
    pub(super) order_slots: Vec<usize>,
    pub(super) functions: Vec<HirWindowFunction<'a>>,
    late_inputs: Vec<HirWindowLateInput<'a>>,
}

impl HirWindowBufferPlan<'_> {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn partition_key_info(&self) -> impl Iterator<Item = Result<KeyInfo>> + '_ {
        self.window
            .partition_by
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                let slot = self.partition_slots[*index];
                !self.partition_slots[..*index].contains(&slot)
            })
            .map(|(_, term)| {
                Ok(KeyInfo {
                    sort_order: turso_parser::ast::SortOrder::Asc,
                    collation: term
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value())
                        .unwrap_or_default(),
                    nulls_order: None,
                })
            })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn order_key_info(&self) -> impl Iterator<Item = Result<KeyInfo>> + '_ {
        self.window.order_by.iter().map(|term| {
            Ok(KeyInfo {
                sort_order: turso_parser::ast::SortOrder::Asc,
                collation: term
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
                    .unwrap_or_default(),
                nulls_order: None,
            })
        })
    }

    fn needs_start_cursor(&self) -> bool {
        !matches!(
            self.window.frame.start,
            hir::WindowFrameBound::UnboundedPreceding
        )
    }

    fn needs_app_cursor(&self) -> bool {
        self.window.frame.exclude.is_some()
            || self.functions.iter().any(|function| {
                matches!(
                    &function.function,
                    AccumulatorFunc::Window(
                        WindowFunc::FirstValue
                            | WindowFunc::NthValue
                            | WindowFunc::Lag
                            | WindowFunc::Lead
                    )
                )
            })
    }

    fn needs_positional_tracking(&self) -> bool {
        self.functions.iter().any(|function| {
            matches!(
                &function.function,
                AccumulatorFunc::Window(WindowFunc::FirstValue | WindowFunc::NthValue)
            )
        })
    }
}

struct WindowCallCollector<'a> {
    block: hir::QueryBlockId,
    calls: Vec<Option<&'a hir::FunctionCall>>,
}

impl<'expr> ExprVisitor<'expr> for WindowCallCollector<'expr> {
    type Context = ();
    type Output = ();
    type Error = LimboError;

    fn pre_order(
        &mut self,
        _parent: &'expr hir::Expr,
        _context: &mut (),
        _child_index: usize,
        _child: &'expr hir::Expr,
    ) -> Result<ControlFlow<(), ()>> {
        Ok(ControlFlow::Continue(()))
    }

    fn post_order(
        &mut self,
        expression: &'expr hir::Expr,
        _context: (),
        _children: &[()],
    ) -> Result<()> {
        let hir::Expr::Function(call) = expression else {
            return Ok(());
        };
        let hir::FunctionEvaluation::Window { id, .. } = call.evaluation else {
            return Ok(());
        };
        if id.block != self.block || id.index >= self.calls.len() {
            return Err(LimboError::InternalError(format!(
                "HIR window function {id:?} is outside query block {:?}",
                self.block
            )));
        }
        if self.calls[id.index].replace(call).is_some() {
            return Err(LimboError::InternalError(format!(
                "HIR window function {id:?} has more than one definition"
            )));
        }
        Ok(())
    }
}

struct LateArgumentCollector<'columns, 'expr> {
    owner: &'expr hir::Expr,
    columns: &'columns mut Vec<HirWindowBufferColumn<'expr>>,
    inputs: &'columns mut Vec<HirWindowLateInput<'expr>>,
}

impl<'expr> ExprVisitor<'expr> for LateArgumentCollector<'_, 'expr> {
    type Context = ();
    type Output = ();
    type Error = LimboError;

    fn child(&mut self, expression: &'expr hir::Expr, index: usize) -> Option<&'expr hir::Expr> {
        if matches!(
            expression,
            hir::Expr::Function(hir::FunctionCall {
                evaluation: hir::FunctionEvaluation::Aggregate { .. },
                ..
            }) | hir::Expr::Subquery(_)
        ) {
            return None;
        }
        expression.child(index)
    }

    fn pre_order(
        &mut self,
        _parent: &'expr hir::Expr,
        _context: &mut (),
        _child_index: usize,
        _child: &'expr hir::Expr,
    ) -> Result<ControlFlow<(), ()>> {
        Ok(ControlFlow::Continue(()))
    }

    fn post_order(
        &mut self,
        expression: &'expr hir::Expr,
        _context: (),
        _children: &[()],
    ) -> Result<()> {
        let column = match expression {
            hir::Expr::Column(reference) => HirWindowBufferColumn::Source {
                expression,
                reference: *reference,
            },
            hir::Expr::RowId(_)
            | hir::Expr::Subquery(_)
            | hir::Expr::Function(hir::FunctionCall {
                evaluation: hir::FunctionEvaluation::Aggregate { .. },
                ..
            }) => HirWindowBufferColumn::Evaluate(expression),
            _ => return Ok(()),
        };
        let column = push_reused_column(self.columns, column);
        self.inputs.push(HirWindowLateInput {
            owner: self.owner,
            expression,
            column,
        });
        Ok(())
    }
}

fn columns_are_equivalent(
    left: &HirWindowBufferColumn<'_>,
    right: &HirWindowBufferColumn<'_>,
) -> bool {
    match (left, right) {
        (
            HirWindowBufferColumn::Source {
                reference: left, ..
            },
            HirWindowBufferColumn::Source {
                reference: right, ..
            },
        ) => left == right,
        (HirWindowBufferColumn::Evaluate(left), HirWindowBufferColumn::Evaluate(right)) => {
            left.equivalent(right)
        }
        (
            HirWindowBufferColumn::Source { reference, .. },
            HirWindowBufferColumn::Evaluate(expression),
        )
        | (
            HirWindowBufferColumn::Evaluate(expression),
            HirWindowBufferColumn::Source { reference, .. },
        ) => {
            matches!(expression, hir::Expr::Column(other) if other == reference)
        }
    }
}

fn push_reused_column<'a>(
    columns: &mut Vec<HirWindowBufferColumn<'a>>,
    column: HirWindowBufferColumn<'a>,
) -> usize {
    if let Some(index) = columns
        .iter()
        .position(|existing| columns_are_equivalent(existing, &column))
    {
        return index;
    }
    let index = columns.len();
    columns.push(column);
    index
}

fn expression_column(expression: &hir::Expr) -> HirWindowBufferColumn<'_> {
    match expression {
        hir::Expr::Column(reference) => HirWindowBufferColumn::Source {
            expression,
            reference: *reference,
        },
        _ => HirWindowBufferColumn::Evaluate(expression),
    }
}

fn collect_window_calls<'a>(
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
) -> Result<Vec<&'a hir::FunctionCall>> {
    let mut collector = WindowCallCollector {
        block: block.id,
        calls: (0..block.window_function_count).map(|_| None).collect(),
    };
    let mut collect = |expression: &'a hir::Expr| expression.walk((), &mut collector);
    for output in &block.outputs {
        collect(&output.expr)?;
    }
    if let hir::QueryBlockBody::Select {
        filter, grouping, ..
    } = &block.body
    {
        if let Some(filter) = filter {
            collect(filter)?;
        }
        if let Some(grouping) = grouping {
            for key in &grouping.keys {
                collect(key)?;
            }
            if let Some(having) = &grouping.having {
                collect(having)?;
            }
        }
    }
    for term in &query.order_by {
        collect(&term.expr)?;
    }
    collector
        .calls
        .into_iter()
        .enumerate()
        .map(|(index, call)| {
            call.ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR query block {:?} has no definition for window function {index}",
                    block.id
                ))
            })
        })
        .collect()
}

fn accumulator_function(call: &hir::FunctionCall) -> Result<AccumulatorFunc> {
    match call.function.value() {
        Func::Window(function) => Ok(AccumulatorFunc::Window(function.clone())),
        _ => hir_aggregate_function(call).map(AccumulatorFunc::Agg),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn plan_hir_window_buffer<'a>(
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
    window_id: hir::WindowId,
) -> Result<HirWindowBufferPlan<'a>> {
    if window_id.block != block.id {
        return Err(LimboError::InternalError(format!(
            "HIR window {window_id:?} is outside query block {:?}",
            block.id
        )));
    }
    let Some(window) = block.windows.get(window_id.index) else {
        return Err(LimboError::InternalError(format!(
            "HIR window {window_id:?} has no definition"
        )));
    };
    if window.id != window_id {
        return Err(LimboError::InternalError(format!(
            "HIR window {window_id:?} does not match its definition {:?}",
            window.id
        )));
    }

    let mut columns = Vec::new();
    let partition_slots = window
        .partition_by
        .iter()
        .map(|term| push_reused_column(&mut columns, expression_column(&term.expr)))
        .collect();
    let order_slots = window
        .order_by
        .iter()
        .map(|term| push_reused_column(&mut columns, expression_column(&term.expr)))
        .collect();

    let calls = collect_window_calls(query, block)?;
    let mut functions = Vec::new();
    let mut late_inputs = Vec::new();
    for call in calls {
        let hir::FunctionEvaluation::Window {
            id,
            window: call_window,
            filter,
        } = &call.evaluation
        else {
            unreachable!("window collection only returns window calls")
        };
        if *call_window != window_id {
            continue;
        }
        let function = accumulator_function(call)?;
        let (values, argument_facts): (&[hir::Expr], &[hir::FunctionArgumentFacts]) =
            match &call.arguments {
                hir::FunctionArguments::Star => (&[], &[]),
                hir::FunctionArguments::Expressions { values, facts, .. } => (values, facts),
                hir::FunctionArguments::OrderedSet { .. } => {
                    return Err(LimboError::InternalError(format!(
                        "HIR window function {id:?} has ordered-set arguments"
                    )));
                }
            };
        if values.len() != argument_facts.len() {
            return Err(LimboError::InternalError(format!(
                "HIR window function {id:?} argument facts do not match its arguments"
            )));
        }

        let arguments = if window_function_uses_subtypes(&function) {
            for value in values {
                value.walk(
                    (),
                    &mut LateArgumentCollector {
                        owner: value,
                        columns: &mut columns,
                        inputs: &mut late_inputs,
                    },
                )?;
            }
            values.iter().map(BufferedWindowValue::Recompute).collect()
        } else {
            values
                .iter()
                .map(|value| {
                    let index = columns.len();
                    columns.push(expression_column(value));
                    BufferedWindowValue::Column(index)
                })
                .collect()
        };
        let filter = filter.as_deref().map(|filter| {
            let index = columns.len();
            columns.push(expression_column(filter));
            BufferedWindowValue::Column(index)
        });
        functions.push(HirWindowFunction {
            id: *id,
            function,
            arguments,
            filter,
            argument_facts,
        });
    }

    Ok(HirWindowBufferPlan {
        window,
        columns,
        partition_slots,
        order_slots,
        functions,
        late_inputs,
    })
}

/// Lower one source row into the exact register layout stored by the window
/// buffer. Each slot keeps and lowers its original resolved HIR expression.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_input_row(
    program: &mut ProgramBuilder,
    document: &hir::HirDocument,
    plan: &HirWindowBufferPlan<'_>,
) -> Result<WindowBufferInput> {
    let row = WindowBufferInput {
        start: program.alloc_registers(plan.columns.len()),
        count: plan.columns.len(),
    };
    for (offset, column) in plan.columns.iter().enumerate() {
        translate_expr(program, document, column.expression(), row.start + offset)?;
    }
    Ok(row)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_order_keys(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
    input: WindowBufferInput,
    output_start: usize,
) -> Result<()> {
    emit_window_key_gather(
        program,
        input,
        plan.order_slots.iter().copied(),
        output_start,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_cursor_keys(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
    cursor: CursorID,
    output_start: usize,
) -> usize {
    emit_window_cursor_keys(
        program,
        cursor,
        plan.order_slots.iter().copied(),
        output_start,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn prepare_hir_window_peer_state(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
) -> WindowPeerState {
    prepare_window_peer_state(
        program,
        plan.order_slots.len(),
        plan.window.frame.mode != turso_parser::ast::FrameMode::Rows,
        plan.needs_start_cursor(),
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_peer_change(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
    state: WindowPeerState,
    target_if_peer: BranchOffset,
) -> Result<()> {
    emit_window_peer_change(
        program,
        WindowPeerComparison::from_registers(state.key_count, state.current, state.previous_input),
        plan.order_key_info(),
        target_if_peer,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn prepare_hir_window_input_state(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
) -> WindowInputState {
    prepare_window_input_state(program, plan.partition_key_info().count())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_partition_change(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
    input: WindowBufferInput,
    state: WindowInputState,
    flush_buffer: BranchOffset,
) -> Result<()> {
    let Some(previous_keys) = state.previous_partition else {
        return Ok(());
    };
    emit_window_partition_change(
        program,
        input.start,
        WindowPartitionState {
            previous_keys,
            rowid: state.rowid,
            flush_return: state.flush_return,
        },
        flush_buffer,
        plan.partition_key_info(),
    )
}

fn hir_window_buffer_table(window: hir::WindowId, column_count: usize) -> Arc<BTreeTable> {
    // HIR lowering carries type and collation facts separately. These columns
    // only describe the width of the ephemeral records stored by the cursor.
    let columns = (0..column_count)
        .map(|_| {
            Column::new(
                None,
                String::new(),
                None,
                None,
                Type::Null,
                None,
                ColDef::default(),
            )
        })
        .collect();
    Arc::new(BTreeTable::new(
        0,
        format!(
            "window_buffer_{}_{}_{}",
            window.block.query, window.block.index, window.index
        ),
        Vec::new(),
        columns,
        BTreeCharacteristics::HAS_ROWID,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    ))
}

/// Open the ephemeral cursor roles required by one resolved HIR window.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn open_hir_window_buffer(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
) -> WindowBufferRuntime {
    let table = hir_window_buffer_table(plan.window.id, plan.columns.len());
    open_window_buffer(
        program,
        table,
        plan.needs_start_cursor(),
        plan.needs_app_cursor(),
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn prepare_hir_window_runtime<'a>(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'a>,
) -> Result<Vec<WindowFunctionRuntime<&'a hir::Expr>>> {
    if plan.functions.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR window {:?} has no functions",
            plan.window.id
        )));
    }

    let moving_start = !matches!(
        plan.window.frame.start,
        hir::WindowFrameBound::UnboundedPreceding
    );
    let result_registers_start = program.alloc_registers(plan.functions.len());
    let mut runtimes = Vec::with_capacity(plan.functions.len());
    for (ordinal, function) in plan.functions.iter().enumerate() {
        let argument_collations = function
            .argument_facts
            .iter()
            .map(|facts| {
                facts
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
                    .unwrap_or(CollationSeq::Binary)
            })
            .collect();
        let argument_comparators = function
            .argument_facts
            .iter()
            .map(|facts| custom_type_comparator_from_type_fact(&facts.type_fact))
            .collect();
        let minmax_collation = function
            .argument_facts
            .first()
            .and_then(|facts| facts.collation.as_ref())
            .map(|collation| *collation.value());
        let result_register = result_registers_start + ordinal;
        program.bind_window_result(function.id, result_register);
        runtimes.push(WindowFunctionRuntime::prepare(
            program,
            ordinal,
            WindowFunctionRuntimeSpec {
                function: function.function.clone(),
                arguments: function.arguments.clone(),
                filter: function.filter,
                argument_collations,
                argument_comparators,
                result_register,
                moving_start,
                frame_excludes_rows: plan.window.frame.exclude.is_some(),
                minmax_collation,
            },
        ));
    }
    Ok(runtimes)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_partition_reset(
    program: &mut ProgramBuilder,
    accumulator_start: usize,
    functions: &[WindowFunctionRuntime<&hir::Expr>],
    frame_tracking: WindowFrameTracking,
) {
    emit_window_partition_reset(program, accumulator_start, functions, frame_tracking);
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn prepare_hir_window_frame_tracking(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
) -> WindowFrameTracking {
    prepare_window_frame_tracking(
        program,
        plan.window.frame.exclude.is_some(),
        plan.needs_positional_tracking(),
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn prepare_hir_window_frame_offsets(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
) -> WindowFrameOffsets {
    prepare_window_frame_offsets(
        program,
        matches!(
            plan.window.frame.start,
            hir::WindowFrameBound::Preceding(_) | hir::WindowFrameBound::Following(_)
        ),
        matches!(
            plan.window.frame.end,
            Some(hir::WindowFrameBound::Preceding(_)) | Some(hir::WindowFrameBound::Following(_))
        ),
    )
}

fn hir_window_frame_edge(boundary: &hir::WindowFrameBound) -> WindowFrameEdge {
    match boundary {
        hir::WindowFrameBound::UnboundedPreceding => WindowFrameEdge::UnboundedPreceding,
        hir::WindowFrameBound::Preceding(_) => WindowFrameEdge::Preceding,
        hir::WindowFrameBound::CurrentRow => WindowFrameEdge::CurrentRow,
        hir::WindowFrameBound::Following(_) => WindowFrameEdge::Following,
        hir::WindowFrameBound::UnboundedFollowing => WindowFrameEdge::UnboundedFollowing,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn hir_window_frame_shape(frame: &hir::WindowFrame) -> WindowFrameShape {
    WindowFrameShape {
        mode: frame.mode,
        start: hir_window_frame_edge(&frame.start),
        end: frame
            .end
            .as_ref()
            .map(hir_window_frame_edge)
            .unwrap_or(WindowFrameEdge::CurrentRow),
        has_exclude: frame.exclude.is_some(),
    }
}

fn hir_frame_offset_is_constant(expression: &hir::Expr) -> bool {
    expression.fold(&mut |expression, children| match expression {
        hir::Expr::Literal(
            turso_parser::ast::Literal::CurrentDate
            | turso_parser::ast::Literal::CurrentTime
            | turso_parser::ast::Literal::CurrentTimestamp,
        ) => false,
        hir::Expr::Literal(_) | hir::Expr::Parameter(_) => true,
        hir::Expr::Column(_)
        | hir::Expr::MergedColumn(_)
        | hir::Expr::RowId(_)
        | hir::Expr::Output(_)
        | hir::Expr::Function(_)
        | hir::Expr::Subquery(_)
        | hir::Expr::Array(_)
        | hir::Expr::Subscript { .. }
        | hir::Expr::FieldAccess(_) => false,
        hir::Expr::Binary { .. } => {
            let [lhs, rhs, ..] = children else {
                unreachable!("binary expression has two value children");
            };
            *lhs && *rhs
        }
        hir::Expr::Cast { .. } => {
            let Some(value) = children.first() else {
                unreachable!("cast expression has a value child");
            };
            *value
        }
        hir::Expr::Unary { .. }
        | hir::Expr::Between { .. }
        | hir::Expr::Case { .. }
        | hir::Expr::Collate { .. }
        | hir::Expr::IsNull(_)
        | hir::Expr::NotNull(_)
        | hir::Expr::TruthTest { .. }
        | hir::Expr::InList { .. }
        | hir::Expr::Like { .. }
        | hir::Expr::Row(_)
        | hir::Expr::Raise { .. } => children.iter().all(|constant| *constant),
    })
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_frame_offset(
    program: &mut ProgramBuilder,
    document: &hir::HirDocument,
    expression: &hir::Expr,
    register: usize,
    mode: turso_parser::ast::FrameMode,
    side: WindowFrameBoundSide,
) -> Result<()> {
    emit_window_frame_offset(
        program,
        register,
        mode,
        side,
        hir_frame_offset_is_constant(expression),
        |program, register| {
            translate_expr_no_constant_opt(program, document, expression, register).map(|_| ())
        },
    )
}

/// Evaluate the bounded frame offsets at the start of one partition.
///
/// The runtime decrements these registers while walking the partition, so
/// they must be rebuilt for each partition. Keep start before end to match
/// the legacy window loop.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_frame_offsets(
    program: &mut ProgramBuilder,
    document: &hir::HirDocument,
    plan: &HirWindowBufferPlan<'_>,
    offsets: WindowFrameOffsets,
) -> Result<()> {
    if let Some(register) = offsets.start() {
        let expression = match &plan.window.frame.start {
            hir::WindowFrameBound::Preceding(expression)
            | hir::WindowFrameBound::Following(expression) => expression,
            _ => unreachable!("a start offset is only allocated for a bounded frame start"),
        };
        emit_hir_window_frame_offset(
            program,
            document,
            expression,
            register,
            plan.window.frame.mode,
            WindowFrameBoundSide::Start,
        )?;
    }

    if let Some(register) = offsets.end() {
        let expression = match &plan.window.frame.end {
            Some(
                hir::WindowFrameBound::Preceding(expression)
                | hir::WindowFrameBound::Following(expression),
            ) => expression,
            _ => unreachable!("an end offset is only allocated for a bounded frame end"),
        };
        emit_hir_window_frame_offset(
            program,
            document,
            expression,
            register,
            plan.window.frame.mode,
            WindowFrameBoundSide::End,
        )?;
    }

    Ok(())
}

/// Guard RANGE cursors using the resolved frame and prepared buffer state.
#[cfg_attr(not(test), allow(dead_code))]
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_hir_window_range_cursor_guards(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
    op: crate::translate::window::WindowOp,
    countdown: Option<usize>,
    operation_cursor: CursorID,
    end_cursor: CursorID,
    newest_rowid: usize,
    in_flush: bool,
    done: BranchOffset,
) {
    crate::translate::window::emit_window_range_cursor_guards(
        program,
        hir_window_frame_shape(&plan.window.frame),
        op,
        countdown,
        operation_cursor,
        end_cursor,
        newest_rowid,
        in_flush,
        done,
    );
}

/// Keep operation countdowns and RANGE retry labels shared with legacy lowering.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_offset_gate(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
    op: crate::translate::window::WindowOp,
    countdown: Option<usize>,
    current_cursor: CursorID,
    operation_cursor: CursorID,
    done: BranchOffset,
) -> Result<Option<BranchOffset>> {
    crate::translate::window::emit_window_offset_gate(
        program,
        hir_window_frame_shape(&plan.window.frame),
        op,
        countdown,
        current_cursor,
        operation_cursor,
        done,
        |program, comparison, first, offset, second, target| {
            emit_hir_window_range_test(program, plan, comparison, first, offset, second, target)
        },
    )
}

/// Compare buffered RANGE keys using the order and collation frozen in HIR.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_range_test(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
    comparison: crate::translate::window::RangeCmp,
    first: CursorID,
    offset: usize,
    second: CursorID,
    target: BranchOffset,
) -> Result<()> {
    let [term] = plan.window.order_by.as_slice() else {
        unreachable!("RANGE offsets require exactly one ORDER BY expression");
    };
    let [column] = plan.order_slots.as_slice() else {
        unreachable!("RANGE offsets require exactly one ORDER BY buffer slot");
    };
    crate::translate::window::emit_window_range_test(
        program,
        crate::translate::window::WindowRangeKey {
            column: *column,
            sort_order: term.order,
            nulls_order: term.nulls,
            collation: term
                .collation
                .as_ref()
                .map(|collation| *collation.value())
                .unwrap_or_default(),
        },
        comparison,
        first,
        offset,
        second,
        target,
    )
}

/// Use resolved ORDER BY metadata for the shared later-row peer check.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_subsequent_row(
    program: &mut ProgramBuilder,
    plan: &HirWindowBufferPlan<'_>,
    buffer: &WindowBufferRuntime,
    input: WindowBufferInput,
    rowid: usize,
    peers: WindowPeerState,
    step_end: BranchOffset,
) -> Result<()> {
    emit_window_subsequent_row(
        program,
        buffer.cursors,
        input,
        rowid,
        &buffer.table.name,
        plan.window.frame.mode,
        peers,
        plan.order_key_info(),
        step_end,
    )
}

/// Initialize each partition in legacy order before its first buffered row.
#[cfg_attr(not(test), allow(dead_code))]
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_hir_window_first_row(
    program: &mut ProgramBuilder,
    document: &hir::HirDocument,
    plan: &HirWindowBufferPlan<'_>,
    state: WindowFirstRowFrame<'_>,
    peers: WindowPeerState,
    frame_tracking: WindowFrameTracking,
    accumulator_start: usize,
    functions: &[WindowFunctionRuntime<&hir::Expr>],
    emit_row: impl FnMut(&mut ProgramBuilder) -> Result<()>,
) -> Result<()> {
    emit_window_peer_seed(program, peers);
    emit_hir_window_partition_reset(program, accumulator_start, functions, frame_tracking);
    emit_hir_window_first_row_frame(
        program,
        document,
        plan,
        state,
        accumulator_start,
        functions,
        emit_row,
    )
}

/// Rebuild partition offsets before inserting its first row, then use the
/// shared frame sequence with resolved HIR aggregate results.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_first_row_frame(
    program: &mut ProgramBuilder,
    document: &hir::HirDocument,
    plan: &HirWindowBufferPlan<'_>,
    state: WindowFirstRowFrame<'_>,
    accumulator_start: usize,
    functions: &[WindowFunctionRuntime<&hir::Expr>],
    mut emit_row: impl FnMut(&mut ProgramBuilder) -> Result<()>,
) -> Result<()> {
    let frame = hir_window_frame_shape(&plan.window.frame);
    assert_eq!(
        state.frame, frame,
        "first-row state must match the HIR frame"
    );
    emit_hir_window_frame_offsets(program, document, plan, state.offsets)?;
    emit_window_first_row_frame(program, state, |program, output| match output {
        WindowEmptyFrameOutput::Aggregate => {
            // EXCLUDE frames compute results through their full-frame scan.
            if !frame.has_exclude {
                emit_window_aggregate_results(
                    program,
                    accumulator_start,
                    functions,
                    WindowAggregateResultMode::Value,
                );
            }
            Ok(())
        }
        WindowEmptyFrameOutput::Row => emit_row(program),
    })
}

struct HirWindowValueEmitter<'document, 'inputs> {
    document: &'document hir::HirDocument,
    late_inputs: &'inputs [HirWindowLateInput<'document>],
}

impl<'a> WindowValueEmitter<&'a hir::Expr> for HirWindowValueEmitter<'a, '_> {
    fn emit_buffered_value(
        &mut self,
        program: &mut ProgramBuilder,
        cursor: CursorID,
        value: &BufferedWindowValue<&'a hir::Expr>,
        target: usize,
    ) -> Result<()> {
        match value {
            BufferedWindowValue::Column(column) => program.emit_insn(Insn::Column {
                cursor_id: cursor,
                column: *column,
                dest: target,
                default: None,
            }),
            BufferedWindowValue::Recompute(expression) => {
                let mut inputs: SmallVec<[ExprRegisterInput<'a>; 4]> = SmallVec::new();
                for input in self
                    .late_inputs
                    .iter()
                    .filter(|input| std::ptr::eq(input.owner, *expression))
                {
                    let register = program.alloc_register();
                    program.emit_insn(Insn::Column {
                        cursor_id: cursor,
                        column: input.column,
                        dest: register,
                        default: None,
                    });
                    inputs.push(ExprRegisterInput::new(input.expression, register));
                }
                translate_expr_with_inputs(program, self.document, expression, target, &inputs)?;
            }
        }
        Ok(())
    }
}

/// Emit one HIR window step through the representation-neutral frame logic.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_step<'a>(
    program: &mut ProgramBuilder,
    document: &'a hir::HirDocument,
    plan: &HirWindowBufferPlan<'a>,
    functions: &[WindowFunctionRuntime<&'a hir::Expr>],
    context: WindowStepContext,
) -> Result<()> {
    emit_function_step_runtime(
        program,
        functions,
        &mut HirWindowValueEmitter {
            document,
            late_inputs: &plan.late_inputs,
        },
        context,
    )
}

/// Emit one HIR window inverse through the representation-neutral frame logic.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn emit_hir_window_inverse<'a>(
    program: &mut ProgramBuilder,
    document: &'a hir::HirDocument,
    plan: &HirWindowBufferPlan<'a>,
    functions: &[WindowFunctionRuntime<&'a hir::Expr>],
    accumulator_registers_start: usize,
    start_cursor: CursorID,
) -> Result<()> {
    emit_function_inverse_runtime(
        program,
        functions,
        &mut HirWindowValueEmitter {
            document,
            late_inputs: &plan.late_inputs,
        },
        accumulator_registers_start,
        start_cursor,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dialect::SqliteDialect,
        schema::{BTreeTable, PseudoCursorType, Schema},
        sync::Arc,
        translate::semantic::{
            catalog::{SemanticCatalog, SemanticCatalogDatabase},
            context::DoubleQuotedDml,
            hir::{DatabaseId, HirDocument, HirRoot},
            SemanticOptions, SemanticRootInput,
        },
        vdbe::builder::{CursorType, ProgramBuilderOpts, QueryMode, SourceBinding},
        vdbe::insn::InsertFlags,
        SymbolTable, MAIN_DB_ID,
    };
    use turso_parser::parser::Parser;

    fn analyze_sql(sql: &str) -> HirDocument {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value, keep, group_id, sort_key)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let catalog = SemanticCatalog {
            databases: vec![SemanticCatalogDatabase {
                id: DatabaseId::new(MAIN_DB_ID),
                name: "main".to_string(),
                schema: Arc::new(schema),
            }],
            unqualified_database_search_path: vec![DatabaseId::new(MAIN_DB_ID)],
        };
        let command = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains a statement");
        let turso_parser::ast::Cmd::Stmt(statement) = command else {
            panic!("SQL contains a statement command");
        };
        crate::translate::semantic::analyze_root(
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
        .expect("SQL analyzes")
    }

    fn root_query(document: &HirDocument) -> (&hir::Query, &hir::QueryBlock) {
        let HirRoot::Query(root) = &document.root else {
            panic!("root is a query");
        };
        let query = document.query(root.query).expect("root query exists");
        let block = document
            .query_block(query.first)
            .expect("first query block exists");
        (query, block)
    }

    fn program() -> ProgramBuilder {
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 4, 0))
    }

    fn buffer_cursor(program: &mut ProgramBuilder, columns: usize) -> CursorID {
        program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
            column_count: columns,
        }))
    }

    fn column_reference(column: &HirWindowBufferColumn<'_>) -> Option<hir::ColumnRef> {
        match column {
            HirWindowBufferColumn::Source { reference, .. }
            | HirWindowBufferColumn::Evaluate(hir::Expr::Column(reference)) => Some(*reference),
            HirWindowBufferColumn::Evaluate(_) => None,
        }
    }

    #[test]
    fn ordinary_arguments_and_filter_use_buffer_columns() {
        let document = analyze_sql(
            "SELECT sum(value) FILTER (WHERE keep) \
             OVER (PARTITION BY group_id ORDER BY sort_key) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");

        assert_eq!(plan.window.id, block.windows[0].id);
        assert_eq!(plan.columns.len(), 4);
        let source = column_reference(&plan.columns[0]).expect("partition column is buffered");
        assert_eq!(source.column, 2);
        let source = column_reference(&plan.columns[1]).expect("order column is buffered");
        assert_eq!(source.column, 3);
        let source = column_reference(&plan.columns[2]).expect("argument column is buffered");
        assert_eq!(source.column, 0);
        let source = column_reference(&plan.columns[3]).expect("filter column is buffered");
        assert_eq!(source.column, 1);

        let [function] = plan.functions.as_slice() else {
            panic!("one window function is planned");
        };
        assert!(matches!(function.function, AccumulatorFunc::Agg(_)));
        assert!(matches!(
            function.arguments.as_slice(),
            [BufferedWindowValue::Column(2)]
        ));
        assert!(matches!(
            function.filter,
            Some(BufferedWindowValue::Column(3))
        ));
        assert_eq!(function.argument_facts.len(), 1);
    }

    #[test]
    fn partition_and_order_slots_preserve_terms_after_column_reuse() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 PARTITION BY group_id, group_id \
                 ORDER BY sort_key, group_id\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");

        assert_eq!(plan.partition_slots, [0, 0]);
        assert_eq!(plan.order_slots, [1, 0]);
        assert_eq!(plan.columns.len(), 3);
        assert_eq!(
            column_reference(&plan.columns[0])
                .expect("partition column is buffered")
                .column,
            2
        );
        assert_eq!(
            column_reference(&plan.columns[1])
                .expect("order column is buffered")
                .column,
            3
        );
        assert_eq!(
            column_reference(&plan.columns[2])
                .expect("argument column is buffered")
                .column,
            0
        );
    }

    #[test]
    fn hir_window_order_keys_gather_reused_slots_without_collecting() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 PARTITION BY group_id, group_id \
                 ORDER BY sort_key, group_id\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut program = program();
        let input = WindowBufferInput {
            start: program.alloc_registers(plan.columns.len()),
            count: plan.columns.len(),
        };
        let order_keys = program.alloc_registers(plan.order_slots.len());

        emit_hir_window_order_keys(&mut program, &plan, input, order_keys)
            .expect("resolved ORDER BY slots gather");

        let [sort_key, group_id] = program.insns.as_slice() else {
            panic!("two ORDER BY terms emit two copies");
        };
        assert!(matches!(
            sort_key.0,
            Insn::Copy {
                src_reg,
                dst_reg,
                extra_amount: 0,
            } if src_reg == input.start + 1 && dst_reg == order_keys
        ));
        assert!(matches!(
            group_id.0,
            Insn::Copy {
                src_reg,
                dst_reg,
                extra_amount: 0,
            } if src_reg == input.start && dst_reg == order_keys + 1
        ));
    }

    #[test]
    fn hir_window_cursor_keys_read_repeated_reordered_slots() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 PARTITION BY group_id \
                 ORDER BY sort_key, sort_key, group_id\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        assert_eq!(plan.order_slots, [1, 1, 0]);

        let mut program = program();
        let runtime = open_hir_window_buffer(&mut program, &plan);
        let output = program.alloc_registers(plan.order_slots.len());
        let read_start = program.insns.len();

        let count =
            emit_hir_window_cursor_keys(&mut program, &plan, runtime.cursors.csr_current, output);

        assert_eq!(count, 3);
        for ((instruction, _), (column, destination)) in
            program.insns[read_start..]
                .iter()
                .zip([(1, output), (1, output + 1), (0, output + 2)])
        {
            assert!(matches!(
                instruction,
                Insn::Column {
                    cursor_id,
                    column: actual_column,
                    dest,
                    default: None,
                } if *cursor_id == runtime.cursors.csr_current
                    && *actual_column == column
                    && *dest == destination
            ), "unexpected key read {instruction:?}; expected column {column}, register {destination}");
        }
    }

    #[test]
    fn hir_window_peer_state_uses_legacy_frame_gates_and_seed() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 ORDER BY sort_key \
                 RANGE BETWEEN 1 PRECEDING AND CURRENT ROW\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut range_program = program();
        let state = prepare_hir_window_peer_state(&mut range_program, &plan);

        assert_eq!(state.key_count, 1);
        let current = state.current.expect("ORDER BY keys are allocated");
        let previous_input = state.previous_input.expect("RANGE tracks input peer keys");
        assert_eq!(state.cursors.allocated().count(), 3);

        emit_window_peer_seed(&mut range_program, state);

        assert_eq!(range_program.insns.len(), 4);
        assert!(matches!(
            range_program.insns[0].0,
            Insn::Copy {
                src_reg,
                dst_reg,
                extra_amount: 0,
            } if src_reg == current && dst_reg == previous_input
        ));
        for (copy, cursor) in range_program.insns[1..]
            .iter()
            .zip(state.cursors.allocated())
        {
            assert!(matches!(
                copy.0,
                Insn::Copy {
                    src_reg,
                    dst_reg,
                    extra_amount: 0,
                } if src_reg == previous_input && dst_reg == cursor
            ));
        }

        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 ORDER BY sort_key \
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut rows_program = program();
        let state = prepare_hir_window_peer_state(&mut rows_program, &plan);
        assert!(state.current.is_some());
        assert!(state.previous_input.is_none());
        assert_eq!(state.cursors.allocated().count(), 0);

        emit_window_peer_seed(&mut rows_program, state);
        assert!(rows_program.insns.is_empty());
    }

    #[test]
    fn hir_window_peer_change_uses_resolved_keys_and_no_order_rule() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 ORDER BY sort_key COLLATE nocase \
                 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut keyed_program = program();
        let state = prepare_hir_window_peer_state(&mut keyed_program, &plan);
        let current = state.current.expect("ORDER BY keys are allocated");
        let previous = state
            .previous_input
            .expect("RANGE tracks previous input keys");
        let peer = keyed_program.allocate_label();

        emit_hir_window_peer_change(&mut keyed_program, &plan, state, peer)
            .expect("resolved peer keys compare");

        let [compare, jump, copy] = keyed_program.insns.as_slice() else {
            panic!("keyed peer change emits Compare, Jump, and Copy");
        };
        assert!(matches!(
            &compare.0,
            Insn::Compare {
                count: 1,
                key_info,
                ..
            } if key_info[0].collation == CollationSeq::NoCase
        ));
        assert!(matches!(
            jump.0,
            Insn::Jump {
                target_pc_eq,
                ..
            } if target_pc_eq == peer
        ));
        assert!(matches!(
            copy.0,
            Insn::Copy {
                src_reg,
                dst_reg,
                extra_amount: 0,
            } if src_reg == current && dst_reg == previous
        ));

        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut unordered_program = program();
        let state = prepare_hir_window_peer_state(&mut unordered_program, &plan);
        let peer = unordered_program.allocate_label();

        emit_hir_window_peer_change(&mut unordered_program, &plan, state, peer)
            .expect("an unordered partition is one peer group");

        assert!(matches!(
            unordered_program.insns.as_slice(),
            [(Insn::Goto { target_pc }, _)] if *target_pc == peer
        ));
    }

    #[test]
    fn hir_window_key_comparison_uses_resolved_collations() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 PARTITION BY group_id COLLATE nocase, group_id COLLATE nocase \
                 ORDER BY sort_key, group_id COLLATE nocase\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut program = program();
        let left = program.alloc_registers(2);
        let right = program.alloc_registers(2);
        let equal = program.allocate_label();
        let different = program.allocate_label();

        crate::translate::window::emit_window_key_compare(
            &mut program,
            right,
            left,
            plan.order_key_info(),
            equal,
            different,
        )
        .expect("resolved ORDER BY keys compare");

        let [compare, jump] = program.insns.as_slice() else {
            panic!("key comparison emits Compare and Jump");
        };
        assert!(matches!(
            &compare.0,
            Insn::Compare {
                start_reg_a,
                start_reg_b,
                count: 2,
                key_info,
            } if *start_reg_a == left
                && *start_reg_b == right
                && key_info[0].collation == CollationSeq::Binary
                && key_info[1].collation == CollationSeq::NoCase
        ));
        assert!(matches!(
            &jump.0,
            Insn::Jump {
                target_pc_lt,
                target_pc_eq,
                target_pc_gt,
            } if *target_pc_lt == different
                && *target_pc_eq == equal
                && *target_pc_gt == different
        ));

        let partition_keys = plan
            .partition_key_info()
            .collect::<Result<Vec<_>>>()
            .expect("resolved PARTITION BY keys collect for inspection");
        assert_eq!(partition_keys.len(), 1);
        assert_eq!(partition_keys[0].collation, CollationSeq::NoCase);
    }

    #[test]
    fn hir_window_partition_change_uses_shared_runtime_sequence() {
        let document =
            analyze_sql("SELECT sum(value) OVER (PARTITION BY group_id COLLATE nocase) FROM items");
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut program = program();
        let current_keys = program.alloc_register();
        let state = prepare_hir_window_input_state(&mut program, &plan);
        let previous_keys = state
            .previous_partition
            .expect("partition registers are allocated");
        let sequence_start = program.insns.len();
        let flush_buffer = program.allocate_label();

        emit_hir_window_partition_change(
            &mut program,
            &plan,
            WindowBufferInput {
                start: current_keys,
                count: 1,
            },
            state,
            flush_buffer,
        )
        .expect("resolved partition keys drive the shared runtime sequence");

        let [compare, jump, gosub, null, copy] = &program.insns[sequence_start..] else {
            panic!("partition change emits compare, branch, flush, reset, and key copy");
        };
        assert!(matches!(
            &compare.0,
            Insn::Compare {
                start_reg_a,
                start_reg_b,
                count: 1,
                key_info,
            } if *start_reg_a == current_keys
                && *start_reg_b == previous_keys
                && key_info[0].collation == CollationSeq::NoCase
        ));
        assert!(matches!(
            &jump.0,
            Insn::Jump {
                target_pc_lt,
                target_pc_eq,
                target_pc_gt,
            } if target_pc_lt == target_pc_gt && target_pc_eq != target_pc_lt
        ));
        assert!(matches!(
            &gosub.0,
            Insn::Gosub {
                target_pc,
                return_reg,
            } if *target_pc == flush_buffer && *return_reg == state.flush_return
        ));
        assert!(matches!(
            &null.0,
            Insn::Null {
                dest,
                dest_end: None,
            } if *dest == state.rowid
        ));
        assert!(matches!(
            &copy.0,
            Insn::Copy {
                src_reg,
                dst_reg,
                extra_amount: 0,
            } if *src_reg == current_keys && *dst_reg == previous_keys
        ));
    }

    #[test]
    fn hir_window_without_partition_skips_partition_change() {
        let document = analyze_sql("SELECT sum(value) OVER () FROM items");
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut program = program();
        let input = WindowBufferInput {
            start: program.alloc_registers(plan.columns.len()),
            count: plan.columns.len(),
        };
        let state = prepare_hir_window_input_state(&mut program, &plan);
        assert!(state.previous_partition.is_none());
        let sequence_start = program.insns.len();
        let flush_buffer = program.allocate_label();

        emit_hir_window_partition_change(&mut program, &plan, input, state, flush_buffer)
            .expect("a window without PARTITION BY needs no comparison");

        assert_eq!(program.insns.len(), sequence_start);
    }

    #[test]
    fn input_row_lowers_original_expressions_in_buffer_order() {
        let document = analyze_sql(
            "SELECT sum(value) FILTER (WHERE keep) \
             OVER (PARTITION BY group_id ORDER BY sort_key) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let source = column_reference(&plan.columns[0]).expect("partition column is buffered");
        let mut program = program();
        let source_start = program.alloc_registers(4);
        program.bind_source(
            source.source,
            SourceBinding::Registers {
                start: source_start,
                rowid: None,
            },
        );

        let row = emit_hir_window_input_row(&mut program, &document, &plan)
            .expect("window input row lowers");

        assert_eq!(row.count, 4);
        for (offset, source_column) in [2, 3, 0, 1].into_iter().enumerate() {
            assert!(program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::Copy {
                    src_reg,
                    dst_reg,
                    extra_amount: 0,
                } if *src_reg == source_start + source_column && *dst_reg == row.start + offset
            )));
        }
    }

    #[test]
    fn empty_window_input_row_allocates_no_registers() {
        let document = analyze_sql("SELECT row_number() OVER () FROM items");
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        assert!(plan.columns.is_empty());
        let mut program = program();
        let expected_start = program.alloc_registers(0);

        let row = emit_hir_window_input_row(&mut program, &document, &plan)
            .expect("empty window input row lowers");

        assert_eq!(
            row,
            WindowBufferInput {
                start: expected_start,
                count: 0
            }
        );
        assert!(program.insns.is_empty());
    }

    fn assert_hir_buffer(
        sql: &str,
        expected_columns: usize,
        needs_start_cursor: bool,
        needs_app_cursor: bool,
    ) {
        let document = analyze_sql(sql);
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut program = program();

        let runtime = open_hir_window_buffer(&mut program, &plan);

        assert_eq!(runtime.table.columns().len(), expected_columns);
        assert!(runtime.table.has_rowid);
        assert_eq!(runtime.cursors.csr_start.is_some(), needs_start_cursor);
        assert_eq!(runtime.cursors.csr_app.is_some(), needs_app_cursor);
    }

    #[test]
    fn hir_window_buffer_uses_planned_width_and_legacy_cursor_rules() {
        assert_hir_buffer("SELECT row_number() OVER () FROM items", 0, false, false);
        assert_hir_buffer(
            "SELECT sum(value) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM items",
            1,
            true,
            false,
        );
        assert_hir_buffer(
            "SELECT nth_value(value, 1) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM items",
            2,
            false,
            true,
        );
        assert_hir_buffer(
            "SELECT sum(value) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
             EXCLUDE CURRENT ROW) FROM items",
            1,
            false,
            true,
        );
    }

    #[test]
    fn hir_window_frame_cursors_use_shared_rewind_order() {
        fn rewind_cursors(sql: &str) -> (WindowCursors, Vec<CursorID>) {
            let document = analyze_sql(sql);
            let (query, block) = root_query(&document);
            let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                .expect("window buffer plans");
            let mut program = program();
            let runtime = open_hir_window_buffer(&mut program, &plan);
            let sequence_start = program.insns.len();

            emit_window_frame_cursor_rewind(&mut program, runtime.cursors);

            let cursors = program.insns[sequence_start..]
                .iter()
                .map(|(instruction, _)| {
                    let Insn::Rewind { cursor_id, .. } = instruction else {
                        panic!("frame rewind emits only Rewind instructions");
                    };
                    *cursor_id
                })
                .collect();
            (runtime.cursors, cursors)
        }

        let (bounded, bounded_rewinds) = rewind_cursors(
            "SELECT sum(value) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM items",
        );
        assert_eq!(
            bounded_rewinds,
            vec![
                bounded.csr_start.expect("bounded start uses a cursor"),
                bounded.csr_current,
                bounded.csr_end,
            ]
        );

        let (unbounded, unbounded_rewinds) = rewind_cursors("SELECT sum(value) OVER () FROM items");
        assert_eq!(
            unbounded_rewinds,
            vec![unbounded.csr_current, unbounded.csr_end]
        );
    }

    #[test]
    fn hir_window_row_uses_shared_buffer_insertion() {
        let document = analyze_sql("SELECT sum(value) OVER () FROM items");
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let source = column_reference(&plan.columns[0]).expect("argument column is buffered");
        let mut program = program();
        let runtime = open_hir_window_buffer(&mut program, &plan);
        let source_start = program.alloc_register();
        program.bind_source(
            source.source,
            SourceBinding::Registers {
                start: source_start,
                rowid: None,
            },
        );
        let input = emit_hir_window_input_row(&mut program, &document, &plan)
            .expect("window input row lowers");
        let rowid = program.alloc_register();
        let insertion_start = program.insns.len();

        crate::translate::window::emit_window_buffer_insert(
            &mut program,
            &runtime.cursors,
            input,
            rowid,
            &runtime.table.name,
        );

        let [make_record, new_rowid, insert] = &program.insns[insertion_start..] else {
            panic!("window insertion emits exactly three instructions");
        };
        let Insn::MakeRecord {
            start_reg,
            count,
            dest_reg: record,
            index_name: None,
            affinity_str: None,
        } = &make_record.0
        else {
            panic!("window insertion starts by building the input record");
        };
        assert_eq!(*start_reg, crate::vdbe::insn::to_u32(input.start));
        assert_eq!(*count, crate::vdbe::insn::to_u32(input.count));
        assert!(matches!(
            &new_rowid.0,
            Insn::NewRowid {
                cursor,
                rowid_reg,
                prev_largest_reg: 0,
            } if *cursor == runtime.cursors.csr_write && *rowid_reg == rowid
        ));
        assert!(matches!(
            &insert.0,
            Insn::Insert {
                cursor,
                key_reg,
                record_reg,
                flag,
                table_name,
            } if *cursor == runtime.cursors.csr_write
                && *key_reg == rowid
                && *record_reg == usize::try_from(*record).expect("record register fits usize")
                && flag.0 == InsertFlags::new().require_seek().0
                && table_name == &runtime.table.name
        ));
    }

    #[test]
    fn each_effective_window_gets_only_its_functions() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (ORDER BY sort_key), \
                    avg(value) OVER (PARTITION BY group_id) FROM items",
        );
        let (query, block) = root_query(&document);
        assert_eq!(block.windows.len(), 2);

        for window in &block.windows {
            let plan =
                plan_hir_window_buffer(query, block, window.id).expect("window buffer plans");
            let [function] = plan.functions.as_slice() else {
                panic!("one function belongs to each effective window");
            };
            assert_eq!(function.id.block, block.id);
            let hir::FunctionEvaluation::Window {
                window: function_window,
                ..
            } = collect_window_calls(query, block).expect("window calls collect")
                [function.id.index]
                .evaluation
            else {
                panic!("collected function is a window");
            };
            assert_eq!(function_window, window.id);
        }
    }

    #[test]
    fn missing_window_identity_is_rejected() {
        let mut document = analyze_sql("SELECT sum(value) OVER () FROM items");
        let HirRoot::Query(root) = &document.root else {
            panic!("root is a query");
        };
        let query = &mut document.queries[root.query.index()];
        let block = &mut query.blocks[query.first.index];
        block.window_function_count += 1;

        let (query, block) = root_query(&document);
        let error = plan_hir_window_buffer(query, block, block.windows[0].id)
            .expect_err("missing identity is rejected");
        assert!(error
            .to_string()
            .contains("has no definition for window function 1"));
    }

    #[test]
    fn runtime_uses_resolved_collations_and_binds_function_results() {
        let document = analyze_sql(
            "SELECT min(value COLLATE nocase) OVER window_frame, \
                    max(value COLLATE nocase) OVER window_frame FROM items \
             WINDOW window_frame AS \
                    (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        );
        let (query, block) = root_query(&document);
        let mut program = program();
        assert_eq!(block.windows.len(), 2);
        for window in &block.windows {
            let plan =
                plan_hir_window_buffer(query, block, window.id).expect("window buffer plans");
            let runtimes =
                prepare_hir_window_runtime(&mut program, &plan).expect("window runtime prepares");
            let [runtime] = runtimes.as_slice() else {
                panic!("one function belongs to each effective window");
            };
            assert_eq!(runtime.argument_collations(), &[CollationSeq::NoCase]);
            assert!(runtime.has_minmax_state());
            assert_eq!(
                program.window_result_register(plan.functions[0].id),
                Some(runtime.result_register())
            );
        }
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OpenEphemeral { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn hir_window_runtime_uses_shared_aggregate_results() {
        let document = analyze_sql("SELECT sum(value) OVER () FROM items");
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut program = program();
        let functions =
            prepare_hir_window_runtime(&mut program, &plan).expect("window runtime prepares");
        let accumulator = program.alloc_register();
        let result = functions[0].result_register();

        let value_start = program.insns.len();
        emit_window_aggregate_results(
            &mut program,
            accumulator,
            &functions,
            WindowAggregateResultMode::Value,
        );
        let [value] = &program.insns[value_start..] else {
            panic!("value mode reads one accumulator");
        };
        assert!(matches!(
            value.0,
            Insn::AggValue {
                acc_reg,
                dest_reg,
                ..
            } if acc_reg == accumulator && dest_reg == result
        ));

        let finalize_start = program.insns.len();
        emit_window_aggregate_results(
            &mut program,
            accumulator,
            &functions,
            WindowAggregateResultMode::Finalize,
        );
        let [finalize, copy, clear] = &program.insns[finalize_start..] else {
            panic!("finalize mode consumes, copies, and clears the accumulator");
        };
        assert!(matches!(finalize.0, Insn::AggFinal { register, .. } if register == accumulator));
        assert!(matches!(
            copy.0,
            Insn::Copy {
                src_reg,
                dst_reg,
                extra_amount: 0,
            } if src_reg == accumulator && dst_reg == result
        ));
        assert!(matches!(clear.0, Insn::Null { dest, .. } if dest == accumulator));
    }

    #[test]
    fn hir_window_partition_reset_uses_shared_minmax_and_counter_state() {
        let document = analyze_sql(
            "SELECT min(value) OVER (\
                 ORDER BY sort_key \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut program = program();
        let functions =
            prepare_hir_window_runtime(&mut program, &plan).expect("window runtime prepares");
        let accumulator_start = program.alloc_registers(functions.len());
        let frame_counters = program.alloc_registers(2);
        let frame_tracking = WindowFrameTracking::Positional {
            counters: frame_counters,
        };
        let reset_start = program.insns.len();

        emit_hir_window_partition_reset(
            &mut program,
            accumulator_start,
            &functions,
            frame_tracking,
        );

        let [clear, reset_minmax, reset_sequence, reset_start_counter, reset_end_counter] =
            &program.insns[reset_start..]
        else {
            panic!("partition reset clears accumulators, min/max, and frame counters");
        };
        assert!(matches!(
            clear.0,
            Insn::Null {
                dest,
                dest_end: Some(end),
            } if dest == accumulator_start && end == accumulator_start + functions.len() - 1
        ));
        assert!(matches!(reset_minmax.0, Insn::ResetSorter { .. }));
        assert!(matches!(reset_sequence.0, Insn::Integer { value: 0, .. }));
        assert!(matches!(
            reset_start_counter.0,
            Insn::Integer { value: 0, dest } if dest == frame_counters
        ));
        assert!(matches!(
            reset_end_counter.0,
            Insn::Integer { value: 0, dest } if dest == frame_counters + 1
        ));
    }

    #[test]
    fn hir_window_frame_tracking_has_one_explicit_state() {
        fn tracking(sql: &str) -> WindowFrameTracking {
            let document = analyze_sql(sql);
            let (query, block) = root_query(&document);
            let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                .expect("window buffer plans");
            let mut program = program();
            prepare_hir_window_frame_tracking(&mut program, &plan)
        }

        assert!(matches!(
            tracking("SELECT sum(value) OVER () FROM items"),
            WindowFrameTracking::None
        ));
        assert!(matches!(
            tracking("SELECT first_value(value) OVER () FROM items"),
            WindowFrameTracking::Positional { .. }
        ));
        assert!(matches!(
            tracking(
                "SELECT sum(value) OVER (\
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
                     EXCLUDE CURRENT ROW\
                 ) FROM items"
            ),
            WindowFrameTracking::Excluded { .. }
        ));
    }

    #[test]
    fn hir_window_frame_shape_drops_bound_expressions() {
        fn shape(sql: &str) -> WindowFrameShape {
            let document = analyze_sql(sql);
            let (_, block) = root_query(&document);
            hir_window_frame_shape(&block.windows[0].frame)
        }

        assert_eq!(
            shape("SELECT sum(value) OVER () FROM items"),
            WindowFrameShape {
                mode: turso_parser::ast::FrameMode::Range,
                start: WindowFrameEdge::UnboundedPreceding,
                end: WindowFrameEdge::CurrentRow,
                has_exclude: false,
            }
        );
        assert_eq!(
            shape(
                "SELECT sum(value) OVER (\
                     ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING \
                     EXCLUDE CURRENT ROW\
                 ) FROM items"
            ),
            WindowFrameShape {
                mode: turso_parser::ast::FrameMode::Rows,
                start: WindowFrameEdge::Preceding,
                end: WindowFrameEdge::Following,
                has_exclude: true,
            }
        );
    }

    #[test]
    fn hir_window_frame_order_uses_shared_nonempty_branch() {
        fn check(frame: &str) -> WindowFrameOrderCheck {
            let document = analyze_sql(&format!(
                "SELECT sum(value) OVER (ROWS BETWEEN {frame}) FROM items"
            ));
            let (query, block) = root_query(&document);
            let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                .expect("window buffer plans");
            let mut program = program();
            let offsets = prepare_hir_window_frame_offsets(&mut program, &plan);
            window_frame_order_check(hir_window_frame_shape(&plan.window.frame), offsets)
                .expect("same-kind bounded frame needs an order check")
        }

        let preceding = check("2 PRECEDING AND 1 PRECEDING");
        assert!(matches!(
            preceding,
            WindowFrameOrderCheck::EndAtOrBeforeStart { .. }
        ));
        let following = check("1 FOLLOWING AND 2 FOLLOWING");
        assert!(matches!(
            following,
            WindowFrameOrderCheck::EndAtOrAfterStart { .. }
        ));

        let mut program = program();
        let target = program.allocate_label();
        emit_window_nonempty_branch(&mut program, preceding, target);
        emit_window_nonempty_branch(&mut program, following, target);
        let [preceding_branch, following_branch] = program.insns.as_slice() else {
            panic!("one branch is emitted for each frame direction");
        };
        assert!(matches!(preceding_branch.0, Insn::Le { target_pc, .. } if target_pc == target));
        assert!(matches!(following_branch.0, Insn::Ge { target_pc, .. } if target_pc == target));
    }

    #[test]
    fn hir_window_range_cursor_guards_preserve_bounds_and_flush_behavior() {
        use crate::translate::window::WindowOp;
        for mode in ["ROWS", "GROUPS", "RANGE"] {
            for bounds in [
                "2 PRECEDING AND 1 PRECEDING",
                "1 FOLLOWING AND 2 FOLLOWING",
                "1 PRECEDING AND 1 FOLLOWING",
                "UNBOUNDED PRECEDING AND CURRENT ROW",
            ] {
                let document = analyze_sql(&format!(
                    "SELECT sum(value) OVER (ORDER BY sort_key {mode} BETWEEN {bounds}) FROM items"
                ));
                let (query, block) = root_query(&document);
                let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                    .expect("window buffer plans");
                for op in [WindowOp::AggStep, WindowOp::AggInverse, WindowOp::ReturnRow] {
                    for in_flush in [false, true] {
                        for has_offset in [false, true] {
                            let mut program = program();
                            let operation = buffer_cursor(&mut program, 1);
                            let end = buffer_cursor(&mut program, 1);
                            let newest = program.alloc_register();
                            let offset = program.alloc_register();
                            let done = program.allocate_label();
                            emit_hir_window_range_cursor_guards(
                                &mut program,
                                &plan,
                                op,
                                has_offset.then_some(offset),
                                operation,
                                end,
                                newest,
                                in_flush,
                                done,
                            );
                            let guarded = mode == "RANGE"
                                && has_offset
                                && matches!(
                                    bounds,
                                    "2 PRECEDING AND 1 PRECEDING" | "1 FOLLOWING AND 2 FOLLOWING"
                                );
                            let count = if guarded && op == WindowOp::AggInverse {
                                3
                            } else if guarded && op == WindowOp::AggStep && !in_flush {
                                2
                            } else {
                                0
                            };
                            assert_eq!(
                                program.insns.len(),
                                count,
                                "{mode} {bounds} {op:?} flush={in_flush} offset={has_offset}"
                            );
                            if count == 0 {
                                continue;
                            }
                            let Insn::RowId {
                                cursor_id,
                                dest: lhs_reg,
                            } = program.insns[0].0
                            else {
                                panic!("guard reads operation cursor first");
                            };
                            assert_eq!(cursor_id, operation);
                            let rhs_reg = if count == 3 {
                                let Insn::RowId { cursor_id, dest } = program.insns[1].0 else {
                                    panic!("inverse guard reads end cursor second");
                                };
                                assert_eq!(cursor_id, end);
                                assert_ne!(dest, lhs_reg);
                                dest
                            } else {
                                newest
                            };
                            assert!(matches!(&program.insns[count - 1].0,
                                Insn::Ge { lhs, rhs, target_pc, flags, collation }
                                if *lhs == lhs_reg && *rhs == rhs_reg && *target_pc == done
                                    && !flags.has_nulleq() && !flags.has_jump_if_null()
                                    && collation.is_none()
                            ));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn hir_window_offset_gate_skips_missing_offsets_and_uses_countdowns() {
        use crate::translate::window::WindowOp;
        for mode in ["ROWS", "GROUPS", "RANGE"] {
            let document = analyze_sql(&format!(
                "SELECT sum(value) OVER ({mode} \
                 BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM items"
            ));
            let (query, block) = root_query(&document);
            let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                .expect("window buffer plans");
            for op in [WindowOp::AggStep, WindowOp::AggInverse, WindowOp::ReturnRow] {
                let mut program = program();
                let current = buffer_cursor(&mut program, 1);
                let operation = buffer_cursor(&mut program, 1);
                let done = program.allocate_label();
                let countdown = program.alloc_register();
                assert!(emit_hir_window_offset_gate(
                    &mut program,
                    &plan,
                    op,
                    None,
                    current,
                    operation,
                    done,
                )
                .expect("missing offset needs no gate")
                .is_none());
                assert!(program.insns.is_empty());
                if mode != "RANGE" {
                    assert!(emit_hir_window_offset_gate(
                        &mut program,
                        &plan,
                        op,
                        Some(countdown),
                        current,
                        operation,
                        done,
                    )
                    .expect("countdown emits")
                    .is_none());
                    let [instruction] = program.insns.as_slice() else {
                        panic!("ROWS/GROUPS gates emit one countdown instruction");
                    };
                    assert!(
                        matches!(instruction.0, Insn::IfPos { reg, target_pc, decrement_by: 1 }
                        if reg == countdown && target_pc == done)
                    );
                }
            }
        }
    }

    #[test]
    fn hir_window_offset_gate_selects_range_comparison_and_retry_target() {
        use crate::translate::window::{RangeCmp, WindowOp};
        for (frame, op, expected, current_first) in [
            (
                "1 PRECEDING AND CURRENT ROW",
                WindowOp::AggStep,
                RangeCmp::Gt,
                false,
            ),
            (
                "1 PRECEDING AND CURRENT ROW",
                WindowOp::AggInverse,
                RangeCmp::Ge,
                false,
            ),
            (
                "1 FOLLOWING AND 2 FOLLOWING",
                WindowOp::AggInverse,
                RangeCmp::Le,
                true,
            ),
        ] {
            let document = analyze_sql(&format!(
                "SELECT sum(value) OVER (ORDER BY sort_key RANGE BETWEEN {frame}) FROM items"
            ));
            let (query, block) = root_query(&document);
            let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                .expect("window buffer plans");
            let mut program = program();
            let current = buffer_cursor(&mut program, plan.columns.len());
            let operation = buffer_cursor(&mut program, plan.columns.len());
            let offset = program.alloc_register();
            let done = program.allocate_label();
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: offset,
            });
            let start = program.insns.len();
            let retry = emit_hir_window_offset_gate(
                &mut program,
                &plan,
                op,
                Some(offset),
                current,
                operation,
                done,
            )
            .expect("RANGE gate emits")
            .expect("RANGE returns a retry label");
            let instructions = &program.insns[start..];
            let (first, second) = if current_first {
                (current, operation)
            } else {
                (operation, current)
            };
            assert!(
                matches!(instructions[0].0, Insn::Column { cursor_id, .. } if cursor_id == first)
            );
            assert!(
                matches!(instructions[1].0, Insn::Column { cursor_id, .. } if cursor_id == second)
            );
            let (actual, target) = match instructions.last().unwrap().0 {
                Insn::Gt { target_pc, .. } => (RangeCmp::Gt, target_pc),
                Insn::Ge { target_pc, .. } => (RangeCmp::Ge, target_pc),
                Insn::Le { target_pc, .. } => (RangeCmp::Le, target_pc),
                _ => panic!("RANGE gate ends with its boundary comparison"),
            };
            assert_eq!((actual, target), (expected, done));
            assert!(!instructions
                .iter()
                .any(|(instruction, _)| matches!(instruction, Insn::IfPos { .. })));
            program.emit_insn(Insn::Goto { target_pc: retry });
            program.preassign_label_to_next_insn(done);
            program
                .resolve_labels()
                .expect("RANGE retry and done labels resolve");
            assert!(matches!(program.insns.last().unwrap().0,
                Insn::Goto { target_pc: BranchOffset::Offset(position) } if position as usize == start));
        }
    }

    #[test]
    fn hir_window_range_comparison_preserves_ordering_and_numeric_guards() {
        use crate::translate::window::RangeCmp;
        for (order, descending) in [("ASC", false), ("DESC", true)] {
            for nulls in ["", "NULLS FIRST", "NULLS LAST"] {
                let document = analyze_sql(&format!(
                    "SELECT sum(value) OVER (PARTITION BY group_id \
                     ORDER BY sort_key COLLATE nocase {order} {nulls} \
                     RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM items"
                ));
                let (query, block) = root_query(&document);
                let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                    .expect("window buffer plans");
                assert_eq!(
                    plan.order_slots,
                    [1],
                    "ORDER BY follows a distinct partition key"
                );
                for (comparison, descending_comparison) in [
                    (RangeCmp::Lt, RangeCmp::Gt),
                    (RangeCmp::Le, RangeCmp::Ge),
                    (RangeCmp::Gt, RangeCmp::Lt),
                    (RangeCmp::Ge, RangeCmp::Le),
                ] {
                    let mut program = program();
                    let first = buffer_cursor(&mut program, plan.columns.len());
                    let second = buffer_cursor(&mut program, plan.columns.len());
                    let offset = program.alloc_register();
                    let target = program.allocate_label();
                    emit_hir_window_range_test(
                        &mut program,
                        &plan,
                        comparison,
                        first,
                        offset,
                        second,
                        target,
                    )
                    .expect("resolved RANGE key emits");
                    let instructions = &program.insns;
                    let Insn::Column {
                        cursor_id,
                        column: 1,
                        dest: lhs,
                        ..
                    } = instructions[0].0
                    else {
                        panic!("first cursor reads the resolved buffer slot");
                    };
                    assert_eq!(cursor_id, first);
                    let Insn::Column {
                        cursor_id,
                        column: 1,
                        dest: rhs,
                        ..
                    } = instructions[1].0
                    else {
                        panic!("second cursor reads the same resolved buffer slot");
                    };
                    assert_eq!(cursor_id, second);
                    let big_null = (descending && nulls == "NULLS FIRST")
                        || (!descending && nulls == "NULLS LAST");
                    assert_eq!(
                        matches!(instructions[2].0, Insn::NotNull { reg, .. } if reg == lhs),
                        big_null
                    );

                    let arithmetic = instructions
                        .iter()
                        .position(|(instruction, _)| {
                            matches!(instruction, Insn::Add { .. } | Insn::Subtract { .. })
                        })
                        .expect("numeric offset arithmetic is emitted");
                    let (left, right, dest) = match instructions[arithmetic].0 {
                        Insn::Subtract { lhs, rhs, dest } if descending => (lhs, rhs, dest),
                        Insn::Add { lhs, rhs, dest } if !descending => (lhs, rhs, dest),
                        _ => panic!("DESC subtracts offsets; ASC adds them"),
                    };
                    assert_eq!((left, right, dest), (lhs, offset, lhs));
                    let numeric_guard = instructions.iter().position(|(instruction, _)|
                        matches!(instruction, Insn::String8 { value, .. } if value.is_empty())
                    ).expect("text/blob arithmetic guard exists");
                    assert!(numeric_guard < arithmetic);
                    let Insn::String8 { dest: empty, .. } = instructions[numeric_guard].0 else {
                        unreachable!()
                    };
                    assert!(matches!(instructions[numeric_guard + 1].0, Insn::Ge {
                        lhs: guard_lhs, rhs: guard_rhs, collation: None, ..
                    } if guard_lhs == lhs && guard_rhs == empty));

                    let expected = if descending {
                        descending_comparison
                    } else {
                        comparison
                    };
                    let comparisons: Vec<_> = instructions
                        .iter()
                        .enumerate()
                        .filter_map(|(position, (instruction, _))| {
                            let (op, left, right, pc, flags, collation) = match instruction {
                                Insn::Lt {
                                    lhs,
                                    rhs,
                                    target_pc,
                                    flags,
                                    collation,
                                } => (RangeCmp::Lt, lhs, rhs, target_pc, flags, collation),
                                Insn::Le {
                                    lhs,
                                    rhs,
                                    target_pc,
                                    flags,
                                    collation,
                                } => (RangeCmp::Le, lhs, rhs, target_pc, flags, collation),
                                Insn::Gt {
                                    lhs,
                                    rhs,
                                    target_pc,
                                    flags,
                                    collation,
                                } => (RangeCmp::Gt, lhs, rhs, target_pc, flags, collation),
                                Insn::Ge {
                                    lhs,
                                    rhs,
                                    target_pc,
                                    flags,
                                    collation,
                                } => (RangeCmp::Ge, lhs, rhs, target_pc, flags, collation),
                                _ => return None,
                            };
                            if *pc != target {
                                return None;
                            }
                            assert_eq!((op, *left, *right), (expected, lhs, rhs));
                            assert_eq!(*collation, Some(CollationSeq::NoCase));
                            Some((position, flags.has_nulleq()))
                        })
                        .collect();
                    let early = comparison == RangeCmp::Ge;
                    assert_eq!(comparisons.len(), if early { 2 } else { 1 });
                    if early {
                        assert!(comparisons[0].0 < arithmetic);
                        assert!(!comparisons[0].1);
                    }
                    let final_comparison = comparisons.last().unwrap();
                    assert!(final_comparison.0 > arithmetic);
                    assert!(final_comparison.1, "final comparison retains NULL equality");
                    program.preassign_label_to_next_insn(target);
                    program
                        .resolve_labels()
                        .expect("all RANGE branch targets resolve");
                }
            }
        }
    }

    #[test]
    fn hir_window_subsequent_row_buffers_before_peer_check() {
        for mode in ["ROWS", "RANGE", "GROUPS"] {
            for order in ["", "ORDER BY sort_key COLLATE nocase"] {
                let document = analyze_sql(&format!(
                    "SELECT sum(value) OVER ({order} {mode} \
                     BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM items"
                ));
                let (query, block) = root_query(&document);
                let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                    .expect("window buffer plans");
                let mut program = program();
                let buffer = open_hir_window_buffer(&mut program, &plan);
                let peers = prepare_hir_window_peer_state(&mut program, &plan);
                let input = WindowBufferInput {
                    start: program.alloc_registers(plan.columns.len()),
                    count: plan.columns.len(),
                };
                let rowid = program.alloc_register();
                let step_end = program.allocate_label();
                let start = program.insns.len();
                emit_hir_window_subsequent_row(
                    &mut program,
                    &plan,
                    &buffer,
                    input,
                    rowid,
                    peers,
                    step_end,
                )
                .expect("subsequent row emits");

                let [record, next_rowid, insert, rest @ ..] = &program.insns[start..] else {
                    panic!("subsequent row must insert before its peer check");
                };
                assert!(matches!(record.0, Insn::MakeRecord { start_reg, count, .. }
                    if start_reg as usize == input.start && count as usize == input.count));
                assert!(
                    matches!(next_rowid.0, Insn::NewRowid { cursor, rowid_reg, .. }
                    if cursor == buffer.cursors.csr_write && rowid_reg == rowid)
                );
                assert!(matches!(insert.0, Insn::Insert { cursor, key_reg, .. }
                    if cursor == buffer.cursors.csr_write && key_reg == rowid));
                if mode == "ROWS" {
                    assert!(peers.previous_input.is_none());
                    assert!(rest.is_empty(), "ROWS never compares peers");
                } else if order.is_empty() {
                    let [jump] = rest else {
                        panic!("without ORDER BY every row stays buffered until flush");
                    };
                    assert!(matches!(jump.0, Insn::Goto { target_pc } if target_pc == step_end));
                } else {
                    let [compare, jump, save] = rest else {
                        panic!("peer comparison must follow insertion");
                    };
                    assert!(matches!(&compare.0, Insn::Compare {
                        start_reg_a, start_reg_b, count: 1, key_info,
                    } if Some(*start_reg_a) == peers.current
                        && Some(*start_reg_b) == peers.previous_input
                        && key_info[0].collation == CollationSeq::NoCase));
                    assert!(
                        matches!(jump.0, Insn::Jump { target_pc_eq, target_pc_lt, target_pc_gt }
                        if target_pc_eq == step_end && target_pc_lt == target_pc_gt && target_pc_lt != step_end)
                    );
                    assert!(
                        matches!(save.0, Insn::Copy { src_reg, dst_reg, extra_amount: 0 }
                        if Some(src_reg) == peers.current && Some(dst_reg) == peers.previous_input)
                    );
                }
            }
        }
    }

    #[test]
    fn hir_window_first_row_initializes_each_partition_in_legacy_order() {
        for function in ["min(value)", "first_value(value)"] {
            let document = analyze_sql(&format!(
                "SELECT {function} OVER (ORDER BY sort_key \
                 GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM items"
            ));
            let (query, block) = root_query(&document);
            let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                .expect("window buffer plans");
            let mut program = program();
            let runtime = open_hir_window_buffer(&mut program, &plan);
            let functions =
                prepare_hir_window_runtime(&mut program, &plan).expect("window functions prepare");
            let accumulator_start = program.alloc_registers(functions.len());
            let peers = prepare_hir_window_peer_state(&mut program, &plan);
            let tracking = prepare_hir_window_frame_tracking(&mut program, &plan);
            let offsets = prepare_hir_window_frame_offsets(&mut program, &plan);
            let input = WindowBufferInput {
                start: program.alloc_registers(plan.columns.len()),
                count: plan.columns.len(),
            };
            let rowid = program.alloc_register();
            let step_end = program.allocate_label();

            for _ in 0..2 {
                let start = program.insns.len();
                emit_hir_window_first_row(
                    &mut program,
                    &document,
                    &plan,
                    WindowFirstRowFrame {
                        frame: hir_window_frame_shape(&plan.window.frame),
                        offsets,
                        cursors: runtime.cursors,
                        input,
                        rowid,
                        table_name: &runtime.table.name,
                        step_end,
                    },
                    peers,
                    tracking,
                    accumulator_start,
                    &functions,
                    |_| panic!("mixed frame bounds have no empty-frame output callback"),
                )
                .expect("first-row initialization emits");

                let instructions = &program.insns[start..];
                let first_offset = instructions
                    .iter()
                    .position(|(instruction, _)| {
                        matches!(
                            instruction, Insn::Integer { value: 1, dest }
                                if Some(*dest) == offsets.start()
                        )
                    })
                    .expect("start offset is rebuilt");
                let peer_count = 1 + peers.cursors.allocated().count();
                let (seed, reset) = instructions[..first_offset].split_at(peer_count);
                assert!(
                    matches!(seed[0].0, Insn::Copy { src_reg, dst_reg, extra_amount: 0 }
                    if Some(src_reg) == peers.current && Some(dst_reg) == peers.previous_input)
                );
                for (instruction, destination) in seed[1..].iter().zip(peers.cursors.allocated()) {
                    assert!(
                        matches!(instruction.0, Insn::Copy { src_reg, dst_reg, extra_amount: 0 }
                        if Some(src_reg) == peers.previous_input && dst_reg == destination)
                    );
                }
                assert!(
                    matches!(reset[0].0, Insn::Null { dest, dest_end: Some(end) }
                    if dest == accumulator_start && end == accumulator_start + functions.len() - 1)
                );
                match tracking {
                    WindowFrameTracking::Positional { counters } => {
                        assert_eq!(reset.len(), 3);
                        assert!(
                            matches!(reset[1].0, Insn::Integer { value: 0, dest } if dest == counters)
                        );
                        assert!(
                            matches!(reset[2].0, Insn::Integer { value: 0, dest } if dest == counters + 1)
                        );
                    }
                    WindowFrameTracking::None => {
                        assert!(functions[0].has_minmax_state());
                        assert_eq!(reset.len(), 3);
                        assert!(matches!(reset[1].0, Insn::ResetSorter { .. }));
                        assert!(matches!(reset[2].0, Insn::Integer { value: 0, .. }));
                    }
                    WindowFrameTracking::Excluded { .. } => panic!("test has no EXCLUDE clause"),
                }
                let record = instructions
                    .iter()
                    .position(|(instruction, _)| matches!(instruction, Insn::MakeRecord { .. }))
                    .expect("first row is buffered");
                assert!(first_offset < record);
                assert!(matches!(instructions.last().unwrap().0,
                    Insn::Goto { target_pc } if target_pc == step_end));
            }
        }
    }

    #[test]
    fn hir_window_first_row_preserves_legacy_sequence() {
        for (frame, guard, delay, start_cursor) in [
            (
                "ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING",
                Some("ge"),
                true,
                true,
            ),
            (
                "ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING EXCLUDE NO OTHERS",
                Some("ge"),
                true,
                true,
            ),
            (
                "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING",
                Some("le"),
                false,
                true,
            ),
            (
                "RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING",
                None,
                false,
                true,
            ),
            (
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                None,
                false,
                false,
            ),
        ] {
            let document = analyze_sql(&format!(
                "SELECT sum(value) OVER (ORDER BY sort_key {frame}) FROM items"
            ));
            let (query, block) = root_query(&document);
            let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                .expect("window buffer plans");
            let mut program = program();
            let runtime = open_hir_window_buffer(&mut program, &plan);
            let functions =
                prepare_hir_window_runtime(&mut program, &plan).expect("window functions prepare");
            let accumulator_start = program.alloc_registers(functions.len());
            let offsets = prepare_hir_window_frame_offsets(&mut program, &plan);
            let input = WindowBufferInput {
                start: program.alloc_registers(plan.columns.len()),
                count: plan.columns.len(),
            };
            let rowid = program.alloc_register();
            let marker = program.alloc_register();
            let step_end = program.allocate_label();
            let sequence_start = program.insns.len();
            emit_hir_window_first_row_frame(
                &mut program,
                &document,
                &plan,
                WindowFirstRowFrame {
                    frame: hir_window_frame_shape(&plan.window.frame),
                    offsets,
                    cursors: runtime.cursors,
                    input,
                    rowid,
                    table_name: &runtime.table.name,
                    step_end,
                },
                accumulator_start,
                &functions,
                |program| {
                    program.emit_insn(Insn::Integer {
                        value: 2,
                        dest: marker,
                    });
                    Ok(())
                },
            )
            .expect("HIR first-row frame emits");

            let sequence = &program.insns[sequence_start..];
            let insert_start = sequence
                .iter()
                .position(|(instruction, _)| matches!(instruction, Insn::MakeRecord { .. }))
                .expect("first row is buffered");
            let mut offset_positions = Vec::new();
            for register in offsets.start().into_iter().chain(offsets.end()) {
                let position = sequence.iter().position(|(instruction, _)|
                    matches!(instruction, Insn::Integer { dest, .. } if *dest == register)
                ).expect("bounded offset is evaluated");
                assert!(
                    position < insert_start,
                    "offset must precede buffer insertion"
                );
                offset_positions.push(position);
            }
            assert!(offset_positions.windows(2).all(|pair| pair[0] < pair[1]));
            let mut expected = vec!["record", "rowid", "insert"];
            if let Some(branch) = guard {
                expected.push(branch);
                if plan.window.frame.exclude.is_none() {
                    expected.push("aggregate");
                }
                expected.extend(["rewind", "row", "reset", "null", "jump"]);
            }
            if delay {
                expected.push("delay");
            }
            if start_cursor {
                expected.push("rewind");
            }
            expected.extend(["rewind", "rewind", "jump"]);
            let mut rewinds = Vec::new();
            let actual: Vec<_> = sequence[insert_start..]
                .iter()
                .map(|instruction| match &instruction.0 {
                    Insn::MakeRecord { .. } => "record",
                    Insn::NewRowid { .. } => "rowid",
                    Insn::Insert { cursor, .. } => {
                        assert_eq!(*cursor, runtime.cursors.csr_write);
                        "insert"
                    }
                    Insn::Ge { .. } => "ge",
                    Insn::Le { .. } => "le",
                    Insn::AggValue {
                        acc_reg, dest_reg, ..
                    } => {
                        assert_eq!(*acc_reg, accumulator_start);
                        assert_eq!(*dest_reg, functions[0].result_register());
                        "aggregate"
                    }
                    Insn::Integer { value: 2, dest } if *dest == marker => "row",
                    Insn::Rewind { cursor_id, .. } => {
                        rewinds.push(*cursor_id);
                        "rewind"
                    }
                    Insn::ResetSorter { .. } => "reset",
                    Insn::Null { dest, .. } => {
                        assert_eq!(*dest, rowid);
                        "null"
                    }
                    Insn::Goto { target_pc } => {
                        assert_eq!(*target_pc, step_end);
                        "jump"
                    }
                    Insn::Subtract { lhs, rhs, dest } => {
                        assert_eq!(Some(*lhs), offsets.end());
                        assert_eq!(Some(*rhs), offsets.start());
                        assert_eq!(*dest, *rhs);
                        "delay"
                    }
                    other => panic!("unexpected first-row instruction: {other:?}"),
                })
                .collect();
            assert_eq!(actual, expected, "{frame}");
            let mut expected_rewinds = Vec::new();
            if guard.is_some() {
                expected_rewinds.push(runtime.cursors.csr_current);
            }
            expected_rewinds.extend(runtime.cursors.csr_start);
            expected_rewinds.extend([runtime.cursors.csr_current, runtime.cursors.csr_end]);
            assert_eq!(rewinds, expected_rewinds, "{frame}");
        }
    }

    #[test]
    fn hir_window_empty_frame_uses_shared_guard_order() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
            .expect("window buffer plans");
        let mut program = program();
        let offsets = prepare_hir_window_frame_offsets(&mut program, &plan);
        let check = window_frame_order_check(hir_window_frame_shape(&plan.window.frame), offsets)
            .expect("same-kind bounded frame needs an order check");
        let current_cursor = buffer_cursor(&mut program, 1);
        let rowid = program.alloc_register();
        let aggregate_marker = program.alloc_register();
        let row_marker = program.alloc_register();
        let step_end = program.allocate_label();

        emit_window_empty_frame_guard(
            &mut program,
            check,
            WindowEmptyFrameState {
                current_cursor,
                rowid,
                step_end,
            },
            |program, output| {
                let (value, dest) = match output {
                    WindowEmptyFrameOutput::Aggregate => (1, aggregate_marker),
                    WindowEmptyFrameOutput::Row => (2, row_marker),
                };
                program.emit_insn(Insn::Integer { value, dest });
                Ok(())
            },
        )
        .expect("empty-frame guard emits");

        let [branch, aggregate, rewind, row, reset, null, jump] = program.insns.as_slice() else {
            panic!("empty-frame guard preserves the seven legacy operations");
        };
        assert!(matches!(branch.0, Insn::Ge { .. }));
        assert!(matches!(
            aggregate.0,
            Insn::Integer { value: 1, dest } if dest == aggregate_marker
        ));
        assert!(matches!(
            rewind.0,
            Insn::Rewind { cursor_id, .. } if cursor_id == current_cursor
        ));
        assert!(matches!(
            row.0,
            Insn::Integer { value: 2, dest } if dest == row_marker
        ));
        assert!(matches!(
            reset.0,
            Insn::ResetSorter { cursor_id } if cursor_id == current_cursor
        ));
        assert!(matches!(null.0, Insn::Null { dest, .. } if dest == rowid));
        assert!(matches!(jump.0, Insn::Goto { target_pc } if target_pc == step_end));
    }

    #[test]
    fn hir_following_frame_uses_shared_start_delay() {
        fn delay(sql: &str) -> (WindowFrameOffsets, ProgramBuilder) {
            let document = analyze_sql(sql);
            let (query, block) = root_query(&document);
            let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                .expect("window buffer plans");
            let mut program = program();
            let offsets = prepare_hir_window_frame_offsets(&mut program, &plan);
            emit_window_following_start_delay(
                &mut program,
                hir_window_frame_shape(&plan.window.frame),
                offsets,
            );
            (offsets, program)
        }

        let (offsets, program) = delay(
            "SELECT sum(value) OVER (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM items",
        );
        let WindowFrameOffsets::Both { start, end } = offsets else {
            panic!("bounded following frame has both offsets");
        };
        let [subtract] = program.insns.as_slice() else {
            panic!("bounded ROWS following frame emits one delay adjustment");
        };
        assert!(matches!(
            subtract.0,
            Insn::Subtract { lhs, rhs, dest }
                if lhs == end && rhs == start && dest == start
        ));

        let (_, range) = delay(
            "SELECT sum(value) OVER (ORDER BY sort_key RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) \
             FROM items",
        );
        assert!(range.insns.is_empty());

        let (_, preceding) = delay(
            "SELECT sum(value) OVER (ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM items",
        );
        assert!(preceding.insns.is_empty());
    }

    #[test]
    fn hir_window_frame_offsets_have_one_explicit_state() {
        fn offsets(frame: &str) -> WindowFrameOffsets {
            let document = analyze_sql(&format!(
                "SELECT sum(value) OVER (ROWS BETWEEN {frame}) FROM items"
            ));
            let (query, block) = root_query(&document);
            let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
                .expect("window buffer plans");
            let mut program = program();
            prepare_hir_window_frame_offsets(&mut program, &plan)
        }

        assert!(matches!(
            offsets("UNBOUNDED PRECEDING AND CURRENT ROW"),
            WindowFrameOffsets::None
        ));
        assert!(matches!(
            offsets("1 PRECEDING AND CURRENT ROW"),
            WindowFrameOffsets::Start { .. }
        ));
        assert!(matches!(
            offsets("UNBOUNDED PRECEDING AND 1 FOLLOWING"),
            WindowFrameOffsets::End { .. }
        ));
        assert!(matches!(
            offsets("1 PRECEDING AND 1 FOLLOWING"),
            WindowFrameOffsets::Both { .. }
        ));
    }

    #[test]
    fn hir_window_frame_offsets_keep_legacy_constant_rules() {
        fn constant(offset: &str) -> bool {
            let sql = format!(
                "SELECT sum(value) OVER (\
                     ROWS BETWEEN {offset} PRECEDING AND CURRENT ROW\
                 ) FROM items"
            );
            let document = analyze_sql(&sql);
            let (_, block) = root_query(&document);
            let hir::WindowFrameBound::Preceding(expression) = &block.windows[0].frame.start else {
                panic!("test window has a PRECEDING start");
            };
            hir_frame_offset_is_constant(expression)
        }

        assert!(constant("1 + 1"));
        assert!(constant("?1"));
        assert!(!constant("value"));
        assert!(!constant("abs(1)"));
        assert!(!constant("CURRENT_DATE"));
    }

    #[test]
    fn hir_window_frame_offset_uses_shared_runtime_error() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 ROWS BETWEEN value PRECEDING AND CURRENT ROW\
             ) FROM items",
        );
        let (_, block) = root_query(&document);
        let window = &block.windows[0];
        let hir::WindowFrameBound::Preceding(expression) = &window.frame.start else {
            panic!("test window has a PRECEDING start");
        };
        let mut program = program();
        let register = program.alloc_register();
        let start = program.insns.len();

        emit_hir_window_frame_offset(
            &mut program,
            &document,
            expression,
            register,
            window.frame.mode,
            WindowFrameBoundSide::Start,
        )
        .expect("frame offset emits");

        assert!(matches!(
            program.insns[start].0,
            Insn::Null {
                dest,
                dest_end: None,
            } if dest == register
        ));
        assert!(program.insns[start..].iter().any(|instruction| matches!(
            &instruction.0,
            Insn::Halt { description, .. }
                if description == "frame starting offset must be a non-negative integer"
        )));
    }

    #[test]
    fn hir_window_frame_offsets_are_rebuilt_in_legacy_order() {
        let document = analyze_sql(
            "SELECT sum(value) OVER (\
                 ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING\
             ) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan = plan_hir_window_buffer(query, block, block.windows[0].id)
            .expect("window buffer plans");
        let mut program = program();
        let offsets = prepare_hir_window_frame_offsets(&mut program, &plan);
        let WindowFrameOffsets::Both { start, end } = offsets else {
            panic!("both bounded frame edges have offset registers");
        };

        emit_hir_window_frame_offsets(&mut program, &document, &plan, offsets)
            .expect("frame offsets emit");

        let start_value = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::Integer { value: 2, dest } if *dest == start)
            })
            .expect("start offset is evaluated");
        let end_value = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::Integer { value: 3, dest } if *dest == end)
            })
            .expect("end offset is evaluated");
        assert!(start_value < end_value);
        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::Halt { description, .. }
                if description == "frame starting offset must be a non-negative integer"
        )));
        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::Halt { description, .. }
                if description == "frame ending offset must be a non-negative integer"
        )));
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_window_arguments_recompute_from_buffered_source_columns() {
        let document = analyze_sql("SELECT json_group_array(json_array(value)) OVER () FROM items");
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");

        assert!(matches!(
            plan.columns.as_slice(),
            [HirWindowBufferColumn::Source { expression: hir::Expr::Column(expression_reference), reference }]
                if reference.column == 0 && expression_reference == reference
        ));
        let [function] = plan.functions.as_slice() else {
            panic!("one JSON window function is planned");
        };
        assert!(matches!(
            function.arguments.as_slice(),
            [BufferedWindowValue::Recompute(hir::Expr::Function(_))]
        ));
    }

    #[test]
    fn hir_window_tracking_defers_excluded_aggregates_and_counts_after_aggregation() {
        use crate::translate::window::{emit_window_tracked_aggregate, WindowOp};
        let document = analyze_sql(
            "SELECT sum(value) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM items",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        for op in [WindowOp::AggStep, WindowOp::AggInverse] {
            for kind in 0..3 {
                let mut program = program();
                let functions = prepare_hir_window_runtime(&mut program, &plan)
                    .expect("window runtime prepares");
                let cursor = buffer_cursor(&mut program, plan.columns.len());
                let accumulators = program.alloc_registers(functions.len());
                let counters = program.alloc_registers(2);
                let tracking = match kind {
                    0 => WindowFrameTracking::None,
                    1 => WindowFrameTracking::Positional { counters },
                    _ => WindowFrameTracking::Excluded {
                        start_rowid: counters,
                        end_rowid: counters + 1,
                    },
                };
                let start = program.insns.len();
                let mut called = false;
                emit_window_tracked_aggregate(&mut program, tracking, op, |program| {
                    called = true;
                    if op == WindowOp::AggStep {
                        emit_hir_window_step(
                            program,
                            &document,
                            &plan,
                            &functions,
                            WindowStepContext {
                                accumulator_registers_start: accumulators,
                                read_cursor: cursor,
                                current_cursor: cursor,
                                has_exclude: false,
                                custom_types_enabled: true,
                            },
                        )
                    } else {
                        emit_hir_window_inverse(
                            program,
                            &document,
                            &plan,
                            &functions,
                            accumulators,
                            cursor,
                        )
                    }
                })
                .expect("tracked HIR aggregate emits");
                let insns = &program.insns[start..];
                assert_eq!(called, kind != 2);
                let aggregates = insns
                    .iter()
                    .filter(|(insn, _)| match op {
                        WindowOp::AggStep => matches!(insn, Insn::AggStep { .. }),
                        WindowOp::AggInverse => matches!(insn, Insn::AggInverse { .. }),
                        WindowOp::ReturnRow => unreachable!(),
                    })
                    .count();
                assert_eq!(aggregates, usize::from(kind != 2));
                if kind == 0 {
                    assert!(!insns
                        .iter()
                        .any(|(insn, _)| matches!(insn, Insn::AddImm { .. })));
                } else {
                    assert!(
                        matches!(insns.last().unwrap().0, Insn::AddImm { register, value: 1 }
                        if register == counters + usize::from(op == WindowOp::AggStep))
                    );
                    if kind == 2 {
                        assert_eq!(insns.len(), 1);
                    }
                }
            }
        }
    }

    #[test]
    fn hir_window_step_reads_direct_argument_and_filter_columns() {
        let document = analyze_sql(
            "SELECT sum(value) FILTER (WHERE keep) OVER window_frame FROM items \
             WINDOW window_frame AS (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut program = program();
        let functions =
            prepare_hir_window_runtime(&mut program, &plan).expect("window runtime prepares");
        let cursor = buffer_cursor(&mut program, plan.columns.len());
        let accumulator_registers_start = program.alloc_registers(functions.len());

        emit_hir_window_step(
            &mut program,
            &document,
            &plan,
            &functions,
            WindowStepContext {
                accumulator_registers_start,
                read_cursor: cursor,
                current_cursor: cursor,
                has_exclude: false,
                custom_types_enabled: true,
            },
        )
        .expect("HIR window step emits");

        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::ColumnRange {
                cursor_id,
                start_column: 0,
                defaults,
                ..
            } if *cursor_id == cursor && defaults.len() == 2
        )));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::AggStep { .. })));
    }

    #[cfg(feature = "json")]
    #[test]
    fn hir_window_step_and_inverse_recompute_json_from_buffered_leaves() {
        let document = analyze_sql(
            "SELECT json_group_array(json_array(value)) OVER window_frame FROM items \
             WINDOW window_frame AS (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        );
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");
        let mut program = program();
        let functions =
            prepare_hir_window_runtime(&mut program, &plan).expect("window runtime prepares");
        let cursor = buffer_cursor(&mut program, plan.columns.len());
        let accumulator_registers_start = program.alloc_registers(functions.len());

        emit_hir_window_step(
            &mut program,
            &document,
            &plan,
            &functions,
            WindowStepContext {
                accumulator_registers_start,
                read_cursor: cursor,
                current_cursor: cursor,
                has_exclude: false,
                custom_types_enabled: true,
            },
        )
        .expect("HIR window step emits");
        emit_hir_window_inverse(
            &mut program,
            &document,
            &plan,
            &functions,
            accumulator_registers_start,
            cursor,
        )
        .expect("HIR window inverse emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Column { cursor_id, column: 0, .. } if *cursor_id == cursor))
                .count(),
            2
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Function { .. }))
                .count(),
            2
        );
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::AggStep { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::AggInverse { .. })));
    }
}
