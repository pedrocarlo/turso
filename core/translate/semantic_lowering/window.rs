//! Window-buffer planning from resolved HIR.

use std::ops::ControlFlow;

use smallvec::SmallVec;

use crate::{
    function::{AccumulatorFunc, Func},
    translate::{
        aggregation::hir_aggregate_function,
        collate::CollationSeq,
        order_by::custom_type_comparator_from_type_fact,
        semantic::hir::{self, ExprVisitor},
        semantic_lowering::expr::{translate_expr_with_inputs, ExprRegisterInput},
        window::{
            emit_function_inverse_runtime, emit_function_step_runtime,
            window_function_uses_subtypes, BufferedWindowValue, WindowFunctionRuntime,
            WindowFunctionRuntimeSpec, WindowStepContext, WindowValueEmitter,
        },
    },
    vdbe::{builder::ProgramBuilder, insn::Insn, CursorID},
    LimboError, Result,
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
    Source(hir::ColumnRef),
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
    pub(super) functions: Vec<HirWindowFunction<'a>>,
    late_inputs: Vec<HirWindowLateInput<'a>>,
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
            hir::Expr::Column(reference) => HirWindowBufferColumn::Source(*reference),
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
        (HirWindowBufferColumn::Source(left), HirWindowBufferColumn::Source(right)) => {
            left == right
        }
        (HirWindowBufferColumn::Evaluate(left), HirWindowBufferColumn::Evaluate(right)) => {
            left.equivalent(right)
        }
        (HirWindowBufferColumn::Source(reference), HirWindowBufferColumn::Evaluate(expression))
        | (HirWindowBufferColumn::Evaluate(expression), HirWindowBufferColumn::Source(reference)) =>
        {
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
        hir::Expr::Column(reference) => HirWindowBufferColumn::Source(*reference),
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
    for expression in &window.partition_by {
        push_reused_column(&mut columns, expression_column(expression));
    }
    for term in &window.order_by {
        push_reused_column(&mut columns, expression_column(&term.expr));
    }

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
        functions,
        late_inputs,
    })
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
        vdbe::builder::{CursorType, ProgramBuilderOpts, QueryMode},
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
            HirWindowBufferColumn::Source(reference)
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

    #[cfg(feature = "json")]
    #[test]
    fn json_window_arguments_recompute_from_buffered_source_columns() {
        let document = analyze_sql("SELECT json_group_array(json_array(value)) OVER () FROM items");
        let (query, block) = root_query(&document);
        let plan =
            plan_hir_window_buffer(query, block, block.windows[0].id).expect("window buffer plans");

        assert!(matches!(
            plan.columns.as_slice(),
            [HirWindowBufferColumn::Source(reference)] if reference.column == 0
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
