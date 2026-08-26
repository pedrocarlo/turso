//! Window-buffer planning from resolved HIR.

use std::ops::ControlFlow;

use crate::{
    LimboError, Result,
    function::{AccumulatorFunc, Func},
    translate::{
        aggregation::hir_aggregate_function,
        semantic::hir::{self, ExprVisitor},
        window::{BufferedWindowValue, window_function_uses_subtypes},
    },
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
    columns: &'columns mut Vec<HirWindowBufferColumn<'expr>>,
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
        push_reused_column(self.columns, column);
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
                        columns: &mut columns,
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MAIN_DB_ID, SymbolTable,
        dialect::SqliteDialect,
        schema::{BTreeTable, Schema},
        sync::Arc,
        translate::semantic::{
            SemanticOptions, SemanticRootInput,
            catalog::{SemanticCatalog, SemanticCatalogDatabase},
            context::DoubleQuotedDml,
            hir::{DatabaseId, HirDocument, HirRoot},
        },
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
        assert!(
            error
                .to_string()
                .contains("has no definition for window function 1")
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
}
