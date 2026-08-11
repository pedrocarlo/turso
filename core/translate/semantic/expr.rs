//! Parser-expression conversion into resolved HIR expressions.

use smallvec::SmallVec;
use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    context::DoubleQuotedDml,
    hir,
    scope::{ExprCollation, NamePrecedence, ResolvedScopeExpr, Scope},
};
use crate::{
    function::{AggFunc, Func, ScalarFunc, WindowFunc},
    schema::{Type, AUTOINCREMENT_SEQ_PREFIX, SQLITE_SEQUENCE_TABLE_NAME},
    sync::Arc,
    translate::collate::CollationSeq,
    util::normalize_ident,
    vdbe::affinity::Affinity,
    LimboError, Result,
};

/// Clause rules that change expression name visibility.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExprPolicy {
    precedence: NamePrecedence,
    allow_dqs_fallback: bool,
    aggregates: AggregatePolicy,
    allow_windows: bool,
    raise: RaisePolicy,
}

#[derive(Clone, Copy, Debug)]
enum AggregatePolicy {
    Allow,
    RejectFunction,
    RejectMisuse,
}

#[derive(Clone, Copy, Debug)]
enum RaisePolicy {
    AbortOnly,
    Trigger,
}

impl ExprPolicy {
    pub(crate) const fn select(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::Allow,
            allow_windows: true,
            raise: RaisePolicy::AbortOnly,
        }
    }

    pub(crate) const fn where_clause(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourceThenOutput,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
        }
    }

    pub(crate) const fn group_by(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourceThenOutput,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
        }
    }

    pub(crate) const fn having(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::OutputThenSource,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::Allow,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
        }
    }

    pub(crate) const fn order_by(dqs_dml: DoubleQuotedDml, aggregate_query: bool) -> Self {
        Self {
            precedence: NamePrecedence::OutputThenSource,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: if aggregate_query {
                AggregatePolicy::Allow
            } else {
                AggregatePolicy::RejectMisuse
            },
            allow_windows: true,
            raise: RaisePolicy::AbortOnly,
        }
    }

    pub(crate) const fn limit(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
        }
    }

    pub(crate) const fn table_function(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
        }
    }

    pub(crate) const fn without_dqs_fallback(mut self) -> Self {
        self.allow_dqs_fallback = false;
        self
    }

    pub(super) const fn schema_expression() -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: false,
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
        }
    }
}

type ExprChildren = SmallVec<[ResolvedScopeExpr; 3]>;

pub(super) struct QueryFunctionState {
    block: hir::QueryBlockId,
    aggregate_count: usize,
    window_function_count: usize,
    named_windows: Vec<NamedFunctionWindow>,
    windows: Vec<hir::ResolvedWindow>,
}

impl QueryFunctionState {
    pub(super) const fn new(block: hir::QueryBlockId) -> Self {
        Self {
            block,
            aggregate_count: 0,
            window_function_count: 0,
            named_windows: Vec::new(),
            windows: Vec::new(),
        }
    }

    fn allocate_aggregate(&mut self) -> hir::AggregateId {
        let id = hir::AggregateId::new(self.block, self.aggregate_count);
        self.aggregate_count += 1;
        id
    }

    pub(super) const fn aggregate_count(&self) -> usize {
        self.aggregate_count
    }

    fn allocate_window(&mut self) -> hir::WindowFunctionId {
        let id = hir::WindowFunctionId::new(self.block, self.window_function_count);
        self.window_function_count += 1;
        id
    }

    pub(super) const fn window_function_count(&self) -> usize {
        self.window_function_count
    }

    pub(super) fn take_windows(&mut self) -> Vec<hir::ResolvedWindow> {
        std::mem::take(&mut self.windows)
    }

    fn allocate_resolved_window(
        &mut self,
        window: FunctionWindow,
        function: &Func,
    ) -> Result<hir::WindowId> {
        let id = hir::WindowId::new(self.block, self.windows.len());
        self.windows.push(window.into_hir(id, function)?);
        Ok(id)
    }
}

enum FunctionContext<'state> {
    ScalarOnly,
    Query(&'state mut QueryFunctionState),
}

enum FunctionBinding {
    Scalar,
    Aggregate(AggregateBinding),
    Window(WindowBinding),
}

struct AggregateBinding {
    id: hir::AggregateId,
}

struct WindowBinding {
    id: hir::WindowFunctionId,
}

#[derive(Clone)]
struct FunctionOrderTerm {
    value: ResolvedScopeExpr,
    order: ast::SortOrder,
    nulls: Option<ast::NullsOrder>,
}

#[derive(Clone)]
struct FunctionWindow {
    partition_by: ExprChildren,
    order_by: Vec<FunctionOrderTerm>,
    frame: Option<FunctionWindowFrame>,
}

#[derive(Clone)]
struct FunctionWindowFrame {
    mode: ast::FrameMode,
    start: hir::WindowFrameBound,
    end: Option<hir::WindowFrameBound>,
    exclude: Option<ast::FrameExclude>,
}

#[derive(Clone)]
struct NamedFunctionWindow {
    name: String,
    window: FunctionWindow,
    frame: Option<ast::FrameClause>,
}

enum FunctionOver {
    Named(String),
    Inline {
        base: Option<String>,
        window: FunctionWindow,
    },
}

enum NestedFunction {
    Aggregate(String),
    Window(String),
}

impl FunctionWindow {
    fn into_hir(self, id: hir::WindowId, function: &Func) -> Result<hir::ResolvedWindow> {
        let frame = match function {
            Func::Window(window) => match coerced_window_frame(window) {
                Some(frame) => frame,
                None => effective_window_frame(self.frame, self.order_by.len(), function)?,
            },
            _ => effective_window_frame(self.frame, self.order_by.len(), function)?,
        };
        Ok(hir::ResolvedWindow {
            id,
            partition_by: self
                .partition_by
                .into_iter()
                .map(|value| value.expr)
                .collect(),
            order_by: self
                .order_by
                .into_iter()
                .map(FunctionOrderTerm::into_hir)
                .collect(),
            frame,
        })
    }

    fn nested_function(&self) -> Option<NestedFunction> {
        nested_function_iter(&self.partition_by)
            .or_else(|| nested_function_iter(self.order_by.iter().map(|term| &term.value)))
            .or_else(|| {
                let frame = self.frame.as_ref()?;
                nested_function_in_window_bound(&frame.start)
                    .or_else(|| frame.end.as_ref().and_then(nested_function_in_window_bound))
            })
    }
}

fn chain_function_window(
    mut window: FunctionWindow,
    base: &FunctionWindow,
    base_has_frame: bool,
    base_name: &str,
) -> Result<FunctionWindow> {
    if !window.partition_by.is_empty() {
        crate::bail_parse_error!("cannot override PARTITION clause of window: {}", base_name);
    }
    if !base.order_by.is_empty() && !window.order_by.is_empty() {
        crate::bail_parse_error!("cannot override ORDER BY clause of window: {}", base_name);
    }
    if base_has_frame {
        crate::bail_parse_error!(
            "cannot override frame specification of window: {}",
            base_name
        );
    }
    window.partition_by.clone_from(&base.partition_by);
    if window.order_by.is_empty() {
        window.order_by.clone_from(&base.order_by);
    }
    Ok(window)
}

impl FunctionOrderTerm {
    fn into_hir(self) -> hir::OrderTerm {
        hir::OrderTerm {
            collation: self.value.collation.value().cloned(),
            type_fact: self.value.type_fact,
            expr: self.value.expr,
            order: self.order,
            nulls: self.nulls,
        }
    }
}

enum FunctionInput {
    Star,
    Expressions {
        distinctness: Option<ast::Distinctness>,
        values: ExprChildren,
        order_by: Vec<FunctionOrderTerm>,
    },
    OrderedSet {
        function: AggFunc,
        direct: ExprChildren,
        order_by: FunctionOrderTerm,
    },
}

impl FunctionInput {
    fn argument_count(&self) -> usize {
        match self {
            Self::Star => 0,
            Self::Expressions { values, .. } => values.len(),
            Self::OrderedSet { direct, .. } => direct.len() + 1,
        }
    }

    fn facts(&self) -> &[ResolvedScopeExpr] {
        match self {
            Self::Star => &[],
            Self::Expressions { values, .. } => values,
            Self::OrderedSet { direct, .. } => direct,
        }
    }

    fn distinctness(&self) -> Option<ast::Distinctness> {
        match self {
            Self::Star => None,
            Self::Expressions { distinctness, .. } => *distinctness,
            Self::OrderedSet { .. } => None,
        }
    }

    fn order_terms(&self) -> &[FunctionOrderTerm] {
        match self {
            Self::Star => &[],
            Self::Expressions { order_by, .. } => order_by,
            Self::OrderedSet { order_by, .. } => std::slice::from_ref(order_by),
        }
    }

    fn result_facts(&self) -> &[ResolvedScopeExpr] {
        match self {
            Self::OrderedSet { order_by, .. } => std::slice::from_ref(&order_by.value),
            _ => self.facts(),
        }
    }

    fn into_hir(self) -> hir::FunctionArguments {
        match self {
            Self::Star => hir::FunctionArguments::Star,
            Self::Expressions {
                distinctness,
                values,
                order_by,
            } => hir::FunctionArguments::Expressions {
                values: values.into_iter().map(|value| value.expr).collect(),
                distinctness,
                order_by: order_by
                    .into_iter()
                    .map(FunctionOrderTerm::into_hir)
                    .collect(),
            },
            Self::OrderedSet {
                direct, order_by, ..
            } => hir::FunctionArguments::OrderedSet {
                direct: direct.into_iter().map(|value| value.expr).collect(),
                order_by: Box::new(order_by.into_hir()),
            },
        }
    }
}

struct ExprFrame<'a> {
    syntax: &'a ast::Expr,
    next_child: usize,
    resolved_children: ExprChildren,
}

impl<'a> ExprFrame<'a> {
    fn new(syntax: &'a ast::Expr) -> Result<Self> {
        validate_special_function_syntax(syntax)?;
        Ok(Self {
            syntax,
            next_child: 0,
            resolved_children: SmallVec::new(),
        })
    }

    fn next_child(&mut self) -> Option<&'a ast::Expr> {
        let child = match self.syntax {
            ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                (self.next_child == 0).then(|| expressions[0].as_ref())
            }
            ast::Expr::Unary(_, expression)
            | ast::Expr::Collate(expression, _)
            | ast::Expr::IsNull(expression)
            | ast::Expr::NotNull(expression) => (self.next_child == 0).then(|| expression.as_ref()),
            ast::Expr::Binary(lhs, _, rhs) => match self.next_child {
                0 => Some(lhs.as_ref()),
                1 => Some(rhs.as_ref()),
                _ => None,
            },
            ast::Expr::Between {
                lhs, start, end, ..
            } => match self.next_child {
                0 => Some(lhs.as_ref()),
                1 => Some(start.as_ref()),
                2 => Some(end.as_ref()),
                _ => None,
            },
            ast::Expr::InList { lhs, rhs, .. } => match self.next_child {
                0 => Some(lhs.as_ref()),
                index => rhs.get(index - 1).map(Box::as_ref),
            },
            ast::Expr::InSelect { lhs, .. } | ast::Expr::InTable { lhs, .. } => {
                match lhs.as_ref() {
                    ast::Expr::Parenthesized(expressions) if expressions.len() > 1 => {
                        expressions.get(self.next_child).map(Box::as_ref)
                    }
                    lhs => (self.next_child == 0).then_some(lhs),
                }
            }
            ast::Expr::Like {
                lhs, rhs, escape, ..
            } => {
                let lhs_count = like_lhs_count(lhs);
                if self.next_child < lhs_count {
                    match lhs.as_ref() {
                        ast::Expr::Parenthesized(expressions) if lhs_count > 1 => {
                            expressions.get(self.next_child).map(Box::as_ref)
                        }
                        lhs => Some(lhs),
                    }
                } else if self.next_child == lhs_count {
                    Some(rhs.as_ref())
                } else if self.next_child == lhs_count + 1 {
                    escape.as_deref()
                } else {
                    None
                }
            }
            ast::Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => match (base.as_deref(), self.next_child) {
                (Some(base), 0) => Some(base),
                _ => {
                    let index = self.next_child - usize::from(base.is_some());
                    match when_then_pairs.get(index / 2) {
                        Some((when, _)) if index % 2 == 0 => Some(when.as_ref()),
                        Some((_, then)) => Some(then.as_ref()),
                        None if index == when_then_pairs.len() * 2 => else_expr.as_deref(),
                        None => None,
                    }
                }
            },
            ast::Expr::Cast { expr, type_name } => {
                match type_name
                    .as_ref()
                    .and_then(|type_name| type_name.size.as_ref())
                {
                    Some(ast::TypeSize::MaxSize(parameter)) => match self.next_child {
                        0 => Some(parameter.as_ref()),
                        1 => Some(expr.as_ref()),
                        _ => None,
                    },
                    Some(ast::TypeSize::TypeSize(first, second)) => match self.next_child {
                        0 => Some(first.as_ref()),
                        1 => Some(second.as_ref()),
                        2 => Some(expr.as_ref()),
                        _ => None,
                    },
                    None => (self.next_child == 0).then(|| expr.as_ref()),
                }
            }
            ast::Expr::FunctionCall {
                name,
                args,
                order_by,
                within_group,
                filter_over,
                ..
            } => {
                let argument_order_end = args.len() + order_by.len();
                let within_group_end = argument_order_end + within_group.len();
                let filter_end =
                    within_group_end + usize::from(filter_over.filter_clause.is_some());
                if self.next_child < args.len() {
                    args.get(self.next_child).map(Box::as_ref)
                } else if self.next_child < argument_order_end {
                    order_by
                        .get(self.next_child - args.len())
                        .map(|term| term.expr.as_ref())
                } else if self.next_child < within_group_end {
                    within_group
                        .get(self.next_child - argument_order_end)
                        .map(|term| term.expr.as_ref())
                } else if self.next_child == within_group_end && filter_over.filter_clause.is_some()
                {
                    filter_over.filter_clause.as_deref()
                } else {
                    window_child(
                        name,
                        args.len(),
                        filter_over.over_clause.as_ref(),
                        self.next_child - filter_end,
                    )
                }
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                let filter_end = usize::from(filter_over.filter_clause.is_some());
                if self.next_child == 0 && filter_end == 1 {
                    filter_over.filter_clause.as_deref()
                } else {
                    window_child(
                        name,
                        0,
                        filter_over.over_clause.as_ref(),
                        self.next_child - filter_end,
                    )
                }
            }
            ast::Expr::Raise(_, message) => {
                (self.next_child == 0).then(|| message.as_deref()).flatten()
            }
            _ => None,
        };
        if child.is_some() {
            self.next_child += 1;
        }
        child
    }
}

fn window_child<'a>(
    name: &ast::Name,
    argument_count: usize,
    over: Option<&'a ast::Over>,
    index: usize,
) -> Option<&'a ast::Expr> {
    let ast::Over::Window(window) = over? else {
        return None;
    };
    if let Some(expression) = window.partition_by.get(index) {
        return Some(expression.as_ref());
    }
    let index = index - window.partition_by.len();
    if let Some(term) = window.order_by.get(index) {
        return Some(term.expr.as_ref());
    }
    if window_function_ignores_frame(name, argument_count) {
        return None;
    }
    let index = index - window.order_by.len();
    let frame = window.frame_clause.as_ref()?;
    [
        frame_bound_expr(&frame.start),
        frame.end.as_ref().and_then(frame_bound_expr),
    ]
    .into_iter()
    .flatten()
    .nth(index)
}

fn frame_bound_expr(bound: &ast::FrameBound) -> Option<&ast::Expr> {
    match bound {
        ast::FrameBound::Following(expression) | ast::FrameBound::Preceding(expression) => {
            Some(expression.as_ref())
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => None,
    }
}

fn window_function_ignores_frame(name: &ast::Name, argument_count: usize) -> bool {
    match (normalize_ident(name.as_str()).as_str(), argument_count) {
        ("row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist", 0)
        | ("ntile", 1)
        | ("lag" | "lead", 1..=3) => true,
        _ => false,
    }
}

fn function_ignores_user_frame(function: &Func) -> bool {
    matches!(function, Func::Window(window) if coerced_window_frame(window).is_some())
}

fn window_expression_count(
    name: &ast::Name,
    argument_count: usize,
    over: Option<&ast::Over>,
) -> usize {
    let Some(ast::Over::Window(window)) = over else {
        return 0;
    };
    let frame_offsets = if window_function_ignores_frame(name, argument_count) {
        0
    } else {
        window.frame_clause.as_ref().map_or(0, |frame| {
            usize::from(frame_bound_expr(&frame.start).is_some())
                + usize::from(frame.end.as_ref().and_then(frame_bound_expr).is_some())
        })
    };
    window.partition_by.len() + window.order_by.len() + frame_offsets
}

fn take_function_window(
    name: &ast::Name,
    argument_count: usize,
    over: Option<&ast::Over>,
    children: &mut impl Iterator<Item = ResolvedScopeExpr>,
) -> Result<Option<FunctionOver>> {
    let Some(over) = over else {
        return Ok(None);
    };
    let ast::Over::Window(window) = over else {
        let ast::Over::Name(name) = over else {
            unreachable!("OVER has only named and inline forms")
        };
        return Ok(Some(FunctionOver::Named(normalize_ident(name.as_str()))));
    };
    let partition_by = children.take(window.partition_by.len()).collect();
    let order_by = window
        .order_by
        .iter()
        .map(|syntax| FunctionOrderTerm {
            value: children
                .next()
                .expect("window child count includes ORDER BY expression"),
            order: syntax.order.unwrap_or(ast::SortOrder::Asc),
            nulls: syntax.nulls,
        })
        .collect();
    let frame = if window_function_ignores_frame(name, argument_count) {
        None
    } else {
        window
            .frame_clause
            .as_ref()
            .map(|frame| take_function_window_frame(frame, children))
            .transpose()?
    };
    Ok(Some(FunctionOver::Inline {
        base: window
            .base
            .as_ref()
            .map(|name| normalize_ident(name.as_str())),
        window: FunctionWindow {
            partition_by,
            order_by,
            frame,
        },
    }))
}

fn take_function_window_frame(
    frame: &ast::FrameClause,
    children: &mut impl Iterator<Item = ResolvedScopeExpr>,
) -> Result<FunctionWindowFrame> {
    Ok(FunctionWindowFrame {
        mode: frame.mode,
        start: take_window_frame_bound(&frame.start, children),
        end: frame
            .end
            .as_ref()
            .map(|bound| take_window_frame_bound(bound, children)),
        exclude: frame.exclude.clone(),
    })
}

fn take_window_frame_bound(
    bound: &ast::FrameBound,
    children: &mut impl Iterator<Item = ResolvedScopeExpr>,
) -> hir::WindowFrameBound {
    match bound {
        ast::FrameBound::CurrentRow => hir::WindowFrameBound::CurrentRow,
        ast::FrameBound::Following(_) => hir::WindowFrameBound::Following(Box::new(
            children
                .next()
                .expect("window child count includes FOLLOWING offset")
                .expr,
        )),
        ast::FrameBound::Preceding(_) => hir::WindowFrameBound::Preceding(Box::new(
            children
                .next()
                .expect("window child count includes PRECEDING offset")
                .expr,
        )),
        ast::FrameBound::UnboundedFollowing => hir::WindowFrameBound::UnboundedFollowing,
        ast::FrameBound::UnboundedPreceding => hir::WindowFrameBound::UnboundedPreceding,
    }
}

impl<'context, 'catalog, 'ast> Analyzer<'context, 'catalog, 'ast> {
    pub(crate) fn analyze_expr(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::Expr> {
        Ok(self.analyze_resolved_expr(syntax, scope, policy)?.expr)
    }

    pub(super) fn analyze_resolved_expr(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<ResolvedScopeExpr> {
        self.analyze_resolved_expr_with_functions(
            syntax,
            scope,
            policy,
            &mut FunctionContext::ScalarOnly,
        )
    }

    pub(super) fn analyze_query_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut QueryFunctionState,
    ) -> Result<ResolvedScopeExpr> {
        let parent = functions.block.query;
        let mut functions = FunctionContext::Query(functions);
        self.analyze_query_scoped_expr(syntax, scope, policy, parent, &mut functions)
    }

    pub(super) fn analyze_query_scalar_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        parent: hir::QueryId,
    ) -> Result<ResolvedScopeExpr> {
        self.analyze_query_scoped_expr(
            syntax,
            scope,
            policy,
            parent,
            &mut FunctionContext::ScalarOnly,
        )
    }

    fn analyze_query_scoped_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        parent: hir::QueryId,
        functions: &mut FunctionContext<'_>,
    ) -> Result<ResolvedScopeExpr> {
        let mut frames = vec![ExprFrame::new(syntax)?];
        loop {
            if let Some(child) = frames
                .last_mut()
                .expect("root expression frame exists")
                .next_child()
            {
                frames.push(ExprFrame::new(child)?);
                continue;
            }

            let frame = frames.pop().expect("completed expression frame exists");
            let resolved = match frame.syntax {
                ast::Expr::Subquery(_)
                | ast::Expr::Exists(_)
                | ast::Expr::InSelect { .. }
                | ast::Expr::InTable { .. } => {
                    self.build_subquery_expr(frame.syntax, frame.resolved_children, scope, parent)?
                }
                _ => self.build_expr(
                    frame.syntax,
                    frame.resolved_children,
                    scope,
                    policy,
                    functions,
                )?,
            };
            match frames.last_mut() {
                Some(parent) => parent.resolved_children.push(resolved),
                None => return Ok(resolved),
            }
        }
    }

    fn build_subquery_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        children: ExprChildren,
        scope: &Scope,
        parent: hir::QueryId,
    ) -> Result<ResolvedScopeExpr> {
        match syntax {
            ast::Expr::Subquery(select) => {
                expect_no_expr_children(children)?;
                let query = self.analyze_subquery(select, parent, scope)?;
                self.resolve_query_output(query, 0)
            }
            ast::Expr::Exists(select) => {
                expect_no_expr_children(children)?;
                let query = self.analyze_subquery(select, parent, scope)?;
                Ok(computed_expr(
                    hir::Expr::Subquery(hir::SubqueryExpr::Exists(query)),
                    hir::TypeFact::known(Type::Integer),
                    ExprCollation::Absent,
                ))
            }
            ast::Expr::InSelect {
                not, rhs: select, ..
            } => self.build_in_subquery(children, select, *not, scope, parent),
            ast::Expr::InTable { not, rhs, args, .. } => {
                let query = self.analyze_named_relation_query(rhs, args, parent, scope)?;
                self.build_in_query(children, query, *not)
            }
            _ => Err(LimboError::InternalError(
                "subquery expression builder received a non-subquery".to_string(),
            )),
        }
    }

    fn build_in_subquery(
        &mut self,
        lhs: ExprChildren,
        select: &'ast ast::Select,
        negated: bool,
        scope: &Scope,
        parent: hir::QueryId,
    ) -> Result<ResolvedScopeExpr> {
        let query = self.analyze_subquery(select, parent, scope)?;
        self.build_in_query(lhs, query, negated)
    }

    fn build_in_query(
        &self,
        lhs: ExprChildren,
        query: hir::QueryId,
        negated: bool,
    ) -> Result<ResolvedScopeExpr> {
        if lhs.is_empty() {
            return Err(LimboError::InternalError(
                "IN query expression has no left value".to_string(),
            ));
        }
        let output_width = self
            .query(query)
            .ok_or_else(|| LimboError::InternalError(format!("missing semantic query {query}")))?
            .output
            .len();
        if output_width != lhs.len() {
            crate::bail_parse_error!(
                "sub-select returns {output_width} columns - expected {}",
                lhs.len()
            );
        }
        let rhs = (0..output_width)
            .map(|output| self.query_output_fact(query, output))
            .collect::<Result<Vec<_>>>()?;
        let comparison = hir::ComparisonSemantics {
            components: lhs
                .iter()
                .zip(&rhs)
                .map(|(lhs, rhs)| comparison_component(lhs, rhs))
                .collect(),
        };
        let collation =
            lhs.iter()
                .zip(&rhs)
                .fold(ExprCollation::Absent, |collation, (lhs, rhs)| {
                    expression_collation(
                        &expression_collation(&collation, &lhs.collation),
                        &rhs.collation,
                    )
                });
        let lhs = if lhs.len() == 1 {
            lhs.into_iter()
                .next()
                .expect("IN query left side has one value")
                .expr
        } else {
            hir::Expr::Row(lhs.into_iter().map(|value| value.expr).collect())
        };
        Ok(computed_expr(
            hir::Expr::Subquery(hir::SubqueryExpr::In {
                lhs: Box::new(lhs),
                query,
                negated,
                comparison,
            }),
            hir::TypeFact::known(Type::Integer),
            collation,
        ))
    }

    fn resolve_query_output(
        &self,
        query: hir::QueryId,
        output: usize,
    ) -> Result<ResolvedScopeExpr> {
        let definition = self
            .query(query)
            .ok_or_else(|| LimboError::InternalError(format!("missing semantic query {query}")))?;
        let width = definition.output.len();
        if width != 1 {
            crate::bail_parse_error!("sub-select returns {width} columns - expected 1");
        }
        let mut resolved = self.query_output_fact(query, output)?;
        resolved.expr = hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output });
        Ok(resolved)
    }

    fn query_output_fact(&self, query: hir::QueryId, output: usize) -> Result<ResolvedScopeExpr> {
        let definition = self
            .query(query)
            .ok_or_else(|| LimboError::InternalError(format!("missing semantic query {query}")))?;
        let facts = definition
            .blocks
            .first()
            .and_then(|block| block.outputs.get(output))
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "semantic query {} has no output {output}",
                    definition.id
                ))
            })?;
        Ok(ResolvedScopeExpr {
            expr: hir::Expr::Output(facts.id),
            type_fact: facts.type_fact.clone(),
            affinity: facts.affinity,
            has_affinity: facts.has_affinity,
            collation: ExprCollation::output(facts.collation.clone(), facts.collation_is_explicit),
        })
    }

    pub(super) fn analyze_named_windows(
        &mut self,
        definitions: &'ast [ast::WindowDef],
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut QueryFunctionState,
    ) -> Result<()> {
        let parent = functions.block.query;
        for definition in definitions {
            let name = normalize_ident(definition.name.as_str());
            let mut window =
                self.analyze_window_definition(&definition.window, scope, policy, parent)?;
            if let Some(base) = &definition.window.base {
                // SQLite ignores the base on the first WINDOW definition.
                if !functions.named_windows.is_empty() {
                    let base = normalize_ident(base.as_str());
                    let inherited = functions
                        .named_windows
                        .iter()
                        .rfind(|window| window.name == base)
                        .ok_or_else(|| LimboError::ParseError(format!("no such window: {base}")))?;
                    window = chain_function_window(
                        window,
                        &inherited.window,
                        inherited.frame.is_some(),
                        &base,
                    )?;
                }
            }
            functions.named_windows.push(NamedFunctionWindow {
                name,
                window,
                frame: definition.window.frame_clause.clone(),
            });
        }
        Ok(())
    }

    fn analyze_window_definition(
        &mut self,
        window: &'ast ast::Window,
        scope: &Scope,
        policy: ExprPolicy,
        parent: hir::QueryId,
    ) -> Result<FunctionWindow> {
        let mut partition_by = ExprChildren::new();
        for expression in &window.partition_by {
            partition_by.push(self.analyze_query_scalar_expr(expression, scope, policy, parent)?);
        }
        let mut order_by = Vec::with_capacity(window.order_by.len());
        for term in &window.order_by {
            order_by.push(FunctionOrderTerm {
                value: self.analyze_query_scalar_expr(&term.expr, scope, policy, parent)?,
                order: term.order.unwrap_or(ast::SortOrder::Asc),
                nulls: term.nulls,
            });
        }
        Ok(FunctionWindow {
            partition_by,
            order_by,
            frame: None,
        })
    }

    fn analyze_window_frame(
        &mut self,
        frame: &ast::FrameClause,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<FunctionWindowFrame> {
        Ok(FunctionWindowFrame {
            mode: frame.mode,
            start: self.analyze_window_bound(&frame.start, scope, policy)?,
            end: frame
                .end
                .as_ref()
                .map(|bound| self.analyze_window_bound(bound, scope, policy))
                .transpose()?,
            exclude: frame.exclude.clone(),
        })
    }

    fn analyze_window_bound(
        &mut self,
        bound: &ast::FrameBound,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::WindowFrameBound> {
        Ok(match bound {
            ast::FrameBound::CurrentRow => hir::WindowFrameBound::CurrentRow,
            ast::FrameBound::Following(expression) => hir::WindowFrameBound::Following(Box::new(
                self.analyze_resolved_expr(expression, scope, policy)?.expr,
            )),
            ast::FrameBound::Preceding(expression) => hir::WindowFrameBound::Preceding(Box::new(
                self.analyze_resolved_expr(expression, scope, policy)?.expr,
            )),
            ast::FrameBound::UnboundedFollowing => hir::WindowFrameBound::UnboundedFollowing,
            ast::FrameBound::UnboundedPreceding => hir::WindowFrameBound::UnboundedPreceding,
        })
    }

    fn resolve_function_over(
        &mut self,
        over: FunctionOver,
        function: &Func,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &QueryFunctionState,
    ) -> Result<FunctionWindow> {
        match over {
            FunctionOver::Named(name) => {
                let named = functions
                    .named_windows
                    .iter()
                    .rfind(|window| window.name == name)
                    .ok_or_else(|| LimboError::ParseError(format!("no such window: {name}")))?;
                let mut window = named.window.clone();
                if !function_ignores_user_frame(function) {
                    window.frame = named
                        .frame
                        .as_ref()
                        .map(|frame| self.analyze_window_frame(frame, scope, policy))
                        .transpose()?;
                }
                Ok(window)
            }
            FunctionOver::Inline { base: None, window } => Ok(window),
            FunctionOver::Inline {
                base: Some(base),
                window,
            } => {
                let named = functions
                    .named_windows
                    .iter()
                    .rfind(|window| window.name == base)
                    .ok_or_else(|| LimboError::ParseError(format!("no such window: {base}")))?;
                chain_function_window(window, &named.window, named.frame.is_some(), &base)
            }
        }
    }

    fn analyze_resolved_expr_with_functions(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut FunctionContext<'_>,
    ) -> Result<ResolvedScopeExpr> {
        let mut frames = vec![ExprFrame::new(syntax)?];
        loop {
            if let Some(child) = frames
                .last_mut()
                .expect("root expression frame exists")
                .next_child()
            {
                frames.push(ExprFrame::new(child)?);
                continue;
            }

            let frame = frames.pop().expect("completed expression frame exists");
            let resolved = self.build_expr(
                frame.syntax,
                frame.resolved_children,
                scope,
                policy,
                functions,
            )?;
            match frames.last_mut() {
                Some(parent) => parent.resolved_children.push(resolved),
                None => return Ok(resolved),
            }
        }
    }

    fn build_expr(
        &mut self,
        syntax: &ast::Expr,
        children: ExprChildren,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut FunctionContext<'_>,
    ) -> Result<ResolvedScopeExpr> {
        match syntax {
            ast::Expr::Literal(literal) => {
                expect_no_expr_children(children)?;
                self.resolve_atomic_expr(hir::Expr::Literal(literal.clone()), scope)
            }
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                expect_no_expr_children(children)?;
                let expression =
                    match scope.resolve_unqualified(name.as_str(), policy.precedence)? {
                        Some(resolved) => resolved.expr,
                        None if policy.allow_dqs_fallback && name.quoted_with('"') => {
                            hir::Expr::Literal(ast::Literal::String(name.as_literal()))
                        }
                        None => crate::bail_parse_error!("no such column: {}", name.as_str()),
                    };
                self.resolve_atomic_expr(expression, scope)
            }
            ast::Expr::Qualified(table, column) => {
                expect_no_expr_children(children)?;
                let Some(resolved) = scope.resolve_qualified(table.as_str(), column.as_str())?
                else {
                    crate::bail_parse_error!("no such table: {}", table.as_str());
                };
                self.resolve_atomic_expr(resolved.expr, scope)
            }
            ast::Expr::DoublyQualified(database, table, column) => {
                expect_no_expr_children(children)?;
                let Some(database_id) = self.context().database(database.as_str()) else {
                    crate::bail_parse_error!(
                        "no such column: {}.{}.{}",
                        database.as_str(),
                        table.as_str(),
                        column.as_str()
                    );
                };
                let Some(resolved) = scope.resolve_database_qualified(
                    database_id,
                    table.as_str(),
                    column.as_str(),
                )?
                else {
                    crate::bail_parse_error!(
                        "no such column: {}.{}.{}",
                        database.as_str(),
                        table.as_str(),
                        column.as_str()
                    );
                };
                self.resolve_atomic_expr(resolved.expr, scope)
            }
            ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                let [inner] = expect_expr_children(children)?;
                Ok(inner)
            }
            ast::Expr::Variable(variable) => {
                expect_no_expr_children(children)?;
                if variable.col_type.is_some() {
                    return super::analyze::unsupported_select();
                }
                self.resolve_atomic_expr(
                    hir::Expr::Parameter(hir::Parameter {
                        index: variable.index,
                        name: variable.name.as_deref().map(str::to_owned),
                        type_fact: hir::TypeFact::dynamic(),
                    }),
                    scope,
                )
            }
            ast::Expr::Unary(operator, _) => {
                let [inner] = expect_expr_children(children)?;
                let type_fact = match operator {
                    ast::UnaryOperator::Positive | ast::UnaryOperator::Negative => {
                        hir::TypeFact::arithmetic_result(
                            &inner.type_fact,
                            &hir::TypeFact::known(Type::Integer),
                        )
                    }
                    ast::UnaryOperator::BitwiseNot | ast::UnaryOperator::Not => {
                        hir::TypeFact::known(Type::Integer)
                    }
                };
                Ok(computed_expr(
                    hir::Expr::Unary {
                        operator: *operator,
                        expr: Box::new(inner.expr),
                    },
                    type_fact,
                    inner.collation,
                ))
            }
            ast::Expr::Binary(lhs_syntax, operator, rhs_syntax) => {
                let [lhs, rhs] = expect_expr_children(children)?;
                if matches!(rhs_syntax.as_ref(), ast::Expr::Literal(ast::Literal::Null))
                    && matches!(operator, ast::Operator::Is | ast::Operator::IsNot)
                {
                    return Ok(null_test_expr(lhs, *operator == ast::Operator::Is));
                }
                if matches!(lhs_syntax.as_ref(), ast::Expr::Literal(ast::Literal::Null))
                    && matches!(operator, ast::Operator::Is | ast::Operator::IsNot)
                {
                    return Ok(null_test_expr(rhs, *operator == ast::Operator::Is));
                }
                let type_fact = binary_type_fact(*operator, &lhs.type_fact, &rhs.type_fact);
                let array_concat = *operator == ast::Operator::Concat
                    && (lhs.type_fact.is_array() || rhs.type_fact.is_array());
                let comparison = operator
                    .is_comparison()
                    .then(|| comparison_semantics(&lhs, &rhs));
                let collation = expression_collation(&lhs.collation, &rhs.collation);
                Ok(computed_expr(
                    hir::Expr::Binary {
                        lhs: Box::new(lhs.expr),
                        operator: *operator,
                        rhs: Box::new(rhs.expr),
                        array_concat,
                        custom: None,
                        comparison,
                    },
                    type_fact,
                    collation,
                ))
            }
            ast::Expr::Between { not, .. } => {
                let [expr, start, end] = expect_expr_children(children)?;
                let start_comparison = comparison_semantics(&expr, &start);
                let end_comparison = comparison_semantics(&expr, &end);
                let collation = expression_collation(
                    &expression_collation(&expr.collation, &start.collation),
                    &end.collation,
                );
                Ok(computed_expr(
                    hir::Expr::Between {
                        expr: Box::new(expr.expr),
                        negated: *not,
                        start: Box::new(start.expr),
                        end: Box::new(end.expr),
                        start_comparison,
                        end_comparison,
                    },
                    hir::TypeFact::known(Type::Integer),
                    collation,
                ))
            }
            ast::Expr::InList { not, rhs, .. } => {
                if children.len() != rhs.len() + 1 {
                    return Err(LimboError::InternalError(format!(
                        "IN expression expected {} child values, got {}",
                        rhs.len() + 1,
                        children.len()
                    )));
                }
                let mut children = children.into_iter();
                let lhs = children
                    .next()
                    .expect("IN child count includes left expression");
                let values = children.collect::<Vec<_>>();
                let comparisons = values
                    .iter()
                    .map(|value| in_comparison_semantics(&lhs, value))
                    .collect();
                let collation = values.iter().fold(lhs.collation.clone(), |current, value| {
                    expression_collation(&current, &value.collation)
                });
                Ok(computed_expr(
                    hir::Expr::InList {
                        lhs: Box::new(lhs.expr),
                        negated: *not,
                        values: values.into_iter().map(|value| value.expr).collect(),
                        comparisons,
                    },
                    hir::TypeFact::known(Type::Integer),
                    collation,
                ))
            }
            ast::Expr::Like {
                lhs,
                not,
                op,
                escape,
                ..
            } => self.build_like(lhs, *not, *op, escape.is_some(), children),
            ast::Expr::Case {
                base: base_syntax,
                when_then_pairs,
                else_expr: else_syntax,
            } => {
                let expected_children = usize::from(base_syntax.is_some())
                    + when_then_pairs.len() * 2
                    + usize::from(else_syntax.is_some());
                if children.len() != expected_children {
                    return Err(LimboError::InternalError(format!(
                        "CASE expression expected {expected_children} child values, got {}",
                        children.len()
                    )));
                }
                let collation = children
                    .iter()
                    .fold(ExprCollation::Absent, |current, child| {
                        expression_collation(&current, &child.collation)
                    });
                let mut children = children.into_iter();
                let base = base_syntax.as_ref().map(|_| {
                    children
                        .next()
                        .expect("CASE child count includes base expression")
                });
                let when_then = when_then_pairs
                    .iter()
                    .map(|_| {
                        (
                            children.next().expect("CASE child count includes WHEN"),
                            children.next().expect("CASE child count includes THEN"),
                        )
                    })
                    .collect::<Vec<_>>();
                let else_expr = else_syntax.as_ref().map(|_| {
                    children
                        .next()
                        .expect("CASE child count includes ELSE expression")
                });
                let base_comparisons = base.as_ref().map_or_else(Vec::new, |base| {
                    when_then
                        .iter()
                        .map(|(when, _)| comparison_semantics(base, when))
                        .collect()
                });
                let type_fact = hir::TypeFact::selected_value_result(
                    when_then
                        .iter()
                        .map(|(_, then)| &then.type_fact)
                        .chain(else_expr.iter().map(|value| &value.type_fact)),
                );
                Ok(computed_expr(
                    hir::Expr::Case {
                        base: base.map(|value| Box::new(value.expr)),
                        when_then: when_then
                            .into_iter()
                            .map(|(when, then)| (when.expr, then.expr))
                            .collect(),
                        else_expr: else_expr.map(|value| Box::new(value.expr)),
                        base_comparisons,
                    },
                    type_fact,
                    collation,
                ))
            }
            ast::Expr::Cast { type_name, .. } => {
                let parameter_count = cast_parameter_count(type_name.as_ref());
                if children.len() != parameter_count + 1 {
                    return Err(LimboError::InternalError(format!(
                        "CAST expression expected {} child values, got {}",
                        parameter_count + 1,
                        children.len()
                    )));
                }
                let mut children = children.into_iter();
                let parameters = children.by_ref().take(parameter_count).collect::<Vec<_>>();
                let input = children
                    .next()
                    .expect("CAST child count includes input expression");
                let collation = parameters
                    .iter()
                    .fold(input.collation.clone(), |current, value| {
                        expression_collation(&current, &value.collation)
                    });
                let target = self.resolve_cast_target(type_name.as_ref(), parameters)?;
                let type_fact = target.type_fact.clone();
                let affinity = target.affinity;
                Ok(ResolvedScopeExpr {
                    expr: hir::Expr::Cast {
                        expr: Box::new(input.expr),
                        target,
                    },
                    type_fact,
                    affinity,
                    has_affinity: true,
                    collation,
                })
            }
            ast::Expr::FunctionCall {
                name,
                distinctness,
                args,
                order_by,
                within_group,
                filter_over,
            } => {
                let ordered_set = if within_group.is_empty() {
                    if normalize_ident(name.as_str()) == "mode" {
                        crate::bail_parse_error!(
                            "mode() requires a WITHIN GROUP (ORDER BY ...) clause"
                        );
                    }
                    None
                } else {
                    Some(resolve_ordered_set_function(
                        name,
                        args,
                        distinctness.as_ref(),
                        order_by,
                        within_group,
                        filter_over,
                    )?)
                };
                let expected_children = args.len()
                    + order_by.len()
                    + within_group.len()
                    + usize::from(filter_over.filter_clause.is_some())
                    + window_expression_count(name, args.len(), filter_over.over_clause.as_ref());
                if children.len() != expected_children {
                    return Err(LimboError::InternalError(format!(
                        "function expression expected {} child values, got {}",
                        expected_children,
                        children.len()
                    )));
                }
                let mut children = children.into_iter();
                let values = children.by_ref().take(args.len()).collect();
                let argument_order = order_by
                    .iter()
                    .zip(children.by_ref())
                    .map(|(syntax, value)| FunctionOrderTerm {
                        value,
                        order: syntax.order.unwrap_or(ast::SortOrder::Asc),
                        nulls: syntax.nulls,
                    })
                    .collect::<Vec<_>>();
                let within_group = within_group
                    .iter()
                    .zip(children.by_ref())
                    .map(|(syntax, value)| FunctionOrderTerm {
                        value,
                        order: syntax.order.unwrap_or(ast::SortOrder::Asc),
                        nulls: syntax.nulls,
                    })
                    .collect::<Vec<_>>();
                let filter = filter_over.filter_clause.as_ref().map(|_| {
                    children
                        .next()
                        .expect("function child count includes FILTER")
                });
                let window = take_function_window(
                    name,
                    args.len(),
                    filter_over.over_clause.as_ref(),
                    &mut children,
                )?;
                let input = match ordered_set {
                    Some(function) => FunctionInput::OrderedSet {
                        function,
                        direct: values,
                        order_by: within_group
                            .into_iter()
                            .next()
                            .expect("ordered-set syntax requires one ordering expression"),
                    },
                    None => FunctionInput::Expressions {
                        distinctness: *distinctness,
                        values,
                        order_by: argument_order,
                    },
                };
                self.build_function_call(name, input, filter, window, scope, policy, functions)
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                let expected_children = usize::from(filter_over.filter_clause.is_some())
                    + window_expression_count(name, 0, filter_over.over_clause.as_ref());
                if children.len() != expected_children {
                    return Err(LimboError::InternalError(format!(
                        "star function expression expected {expected_children} child values, got {}",
                        children.len()
                    )));
                }
                let mut children = children.into_iter();
                let filter = filter_over.filter_clause.as_ref().map(|_| {
                    children
                        .next()
                        .expect("star function child count includes FILTER")
                });
                let window =
                    take_function_window(name, 0, filter_over.over_clause.as_ref(), &mut children)?;
                self.build_function_call(
                    name,
                    FunctionInput::Star,
                    filter,
                    window,
                    scope,
                    policy,
                    functions,
                )
            }
            ast::Expr::Collate(_, name) => {
                let [inner] = expect_expr_children(children)?;
                let collation = self.resolve_collation(name)?;
                Ok(ResolvedScopeExpr {
                    expr: hir::Expr::Collate {
                        expr: Box::new(inner.expr),
                        collation: collation.clone(),
                    },
                    type_fact: inner.type_fact,
                    affinity: inner.affinity,
                    has_affinity: inner.has_affinity,
                    collation: ExprCollation::Explicit(collation),
                })
            }
            ast::Expr::IsNull(_) => {
                let [inner] = expect_expr_children(children)?;
                Ok(null_test_expr(inner, true))
            }
            ast::Expr::NotNull(_) => {
                let [inner] = expect_expr_children(children)?;
                Ok(null_test_expr(inner, false))
            }
            ast::Expr::Raise(action, message) => {
                validate_raise(*action, policy.raise)?;
                let message = match message {
                    Some(_) => {
                        let [message] = expect_expr_children(children)?;
                        Some(Box::new(message.expr))
                    }
                    None => {
                        expect_no_expr_children(children)?;
                        None
                    }
                };
                Ok(computed_expr(
                    hir::Expr::Raise {
                        action: *action,
                        message,
                    },
                    hir::TypeFact::dynamic(),
                    ExprCollation::Absent,
                ))
            }
            _ => super::analyze::unsupported_select(),
        }
    }

    fn build_like(
        &mut self,
        lhs_syntax: &ast::Expr,
        negated: bool,
        operator: ast::LikeOperator,
        has_escape: bool,
        children: ExprChildren,
    ) -> Result<ResolvedScopeExpr> {
        let lhs_count = like_lhs_count(lhs_syntax);
        let expected_children = lhs_count + 1 + usize::from(has_escape);
        if children.len() != expected_children {
            return Err(LimboError::InternalError(format!(
                "{operator} expression expected {expected_children} child values, got {}",
                children.len()
            )));
        }
        if lhs_count > 1 && operator != ast::LikeOperator::Match {
            crate::bail_parse_error!("row value misused");
        }
        if has_escape && operator != ast::LikeOperator::Like {
            crate::bail_parse_error!("wrong number of arguments to function {operator}()");
        }

        let mut children = children.into_iter();
        let lhs = if lhs_count == 1 {
            children
                .next()
                .expect("LIKE child count includes left expression")
                .expr
        } else {
            hir::Expr::Row(
                children
                    .by_ref()
                    .take(lhs_count)
                    .map(|value| value.expr)
                    .collect(),
            )
        };
        let rhs = children
            .next()
            .expect("LIKE child count includes right expression")
            .expr;
        let escape = has_escape.then(|| {
            Box::new(
                children
                    .next()
                    .expect("LIKE child count includes ESCAPE expression")
                    .expr,
            )
        });
        debug_assert!(children.next().is_none());

        let function_name = match operator {
            ast::LikeOperator::Like => "like",
            ast::LikeOperator::Glob => "glob",
            ast::LikeOperator::Regexp => "regexp",
            ast::LikeOperator::Match => {
                #[cfg(all(feature = "fts", not(target_family = "wasm")))]
                {
                    "fts_match"
                }
                #[cfg(any(not(feature = "fts"), target_family = "wasm"))]
                {
                    crate::bail_parse_error!("MATCH requires the 'fts' feature to be enabled")
                }
            }
        };
        let argument_count = lhs_count + 1 + usize::from(has_escape);
        let Some(function) = self
            .context()
            .resolve_function(function_name, argument_count)?
        else {
            crate::bail_parse_error!("no such function: {function_name}");
        };
        let id = self.catalog_object_id(
            None,
            CatalogObjectKind::Function { argument_count },
            function_name,
        );
        let function =
            hir::CatalogObject::new(id, self.context().snapshot(), None, Arc::new(function));

        Ok(computed_expr(
            hir::Expr::Like {
                lhs: Box::new(lhs),
                negated,
                operator,
                function,
                argument_count,
                rhs: Box::new(rhs),
                escape,
            },
            hir::TypeFact::known(Type::Integer),
            ExprCollation::Absent,
        ))
    }

    fn build_function_call(
        &mut self,
        name: &ast::Name,
        input: FunctionInput,
        filter: Option<ResolvedScopeExpr>,
        window: Option<FunctionOver>,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut FunctionContext<'_>,
    ) -> Result<ResolvedScopeExpr> {
        let argument_count = input.argument_count();
        let function_name = normalize_ident(name.as_str());
        let function = match &input {
            FunctionInput::OrderedSet { function, .. } => Func::Agg(function.clone()),
            _ => {
                let Some(function) = self
                    .context()
                    .resolve_function(&function_name, argument_count)?
                else {
                    crate::bail_parse_error!("no such function: {function_name}");
                };
                function
            }
        };
        let special_operation = match self.resolve_custom_type_read(&function, &input)? {
            Some((operation, result_type)) => {
                Some((hir::FunctionOperation::CustomType(operation), result_type))
            }
            None => self.resolve_sequence_operation(&function, &input)?,
        };
        let binding = bind_function(&function, window.is_some(), name, policy, functions)?;
        let aggregate = matches!(binding, FunctionBinding::Aggregate(_));
        let window_evaluation = matches!(binding, FunctionBinding::Window(_));
        let window = match (window_evaluation, window) {
            (true, Some(over)) => Some(match functions {
                FunctionContext::Query(state) => {
                    self.resolve_function_over(over, &function, scope, policy, state)?
                }
                FunctionContext::ScalarOnly => {
                    unreachable!("window binding rejects scalar-only expression contexts")
                }
            }),
            (false, None) => None,
            _ => unreachable!("window binding and OVER clause must agree"),
        };
        if window_evaluation && input.distinctness().is_some() {
            crate::bail_parse_error!("DISTINCT is not supported for window functions");
        }
        if window_evaluation && !input.order_terms().is_empty() {
            crate::bail_parse_error!("ORDER BY clause is not supported yet in aggregate functions");
        }
        if window_evaluation && filter.is_some() && matches!(&function, Func::Window(_)) {
            crate::bail_parse_error!(
                "FILTER clause may only be used with aggregate window functions"
            );
        }
        if (input.distinctness().is_some()
            || !input.order_terms().is_empty()
            || filter.is_some()
            || matches!(&input, FunctionInput::Star))
            && !aggregate
            && !window_evaluation
        {
            return super::analyze::unsupported_select();
        }
        if aggregate || window_evaluation {
            if let Some(nested) = nested_function_iter(input.facts())
                .or_else(|| {
                    nested_function_iter(input.order_terms().iter().map(|term| &term.value))
                })
                .or_else(|| {
                    filter
                        .as_ref()
                        .and_then(|filter| nested_function_iter([filter]))
                })
                .or_else(|| window.as_ref().and_then(FunctionWindow::nested_function))
            {
                match nested {
                    NestedFunction::Aggregate(name) => {
                        crate::bail_parse_error!("misuse of aggregate function {name}()")
                    }
                    NestedFunction::Window(name) => {
                        crate::bail_parse_error!("misuse of window function: {name}()")
                    }
                }
            }
        }
        let (operation, result_type) = special_operation.map_or_else(
            || {
                (
                    hir::FunctionOperation::Ordinary,
                    function_result_type(&function, input.result_facts()),
                )
            },
            |resolved| resolved,
        );
        let id = self.catalog_object_id(
            None,
            CatalogObjectKind::Function { argument_count },
            function_name,
        );
        let function =
            hir::CatalogObject::new(id, self.context().snapshot(), None, Arc::new(function));
        let evaluation = match binding {
            FunctionBinding::Scalar => hir::FunctionEvaluation::Scalar,
            FunctionBinding::Aggregate(binding) => hir::FunctionEvaluation::Aggregate {
                id: binding.id,
                filter: filter.map(|filter| Box::new(filter.expr)),
            },
            FunctionBinding::Window(binding) => hir::FunctionEvaluation::Window {
                id: binding.id,
                window: match functions {
                    FunctionContext::Query(state) => state.allocate_resolved_window(
                        window.expect("window binding requires an OVER clause"),
                        function.value(),
                    )?,
                    FunctionContext::ScalarOnly => {
                        unreachable!("window binding rejects scalar-only expression contexts")
                    }
                },
                filter: filter.map(|filter| Box::new(filter.expr)),
            },
        };
        let arguments = input.into_hir();
        Ok(computed_expr(
            hir::Expr::Function(hir::FunctionCall {
                function,
                evaluation,
                arguments,
                result_type: result_type.clone(),
                operation,
            }),
            result_type,
            ExprCollation::Absent,
        ))
    }

    fn resolve_custom_type_read(
        &mut self,
        function: &Func,
        input: &FunctionInput,
    ) -> Result<Option<(hir::CustomTypeOperation, hir::TypeFact)>> {
        let scalar = match function {
            Func::Scalar(
                scalar @ (ScalarFunc::UnionTagFunc
                | ScalarFunc::UnionExtractFunc
                | ScalarFunc::StructExtractFunc),
            ) => scalar,
            _ => return Ok(None),
        };
        let arguments = input.facts();
        match scalar {
            ScalarFunc::UnionTagFunc => {
                let union_type =
                    custom_argument_type(&arguments[0], |definition| definition.is_union())
                        .ok_or_else(|| {
                            LimboError::ParseError(
                                "union_tag() argument must have a known union type".to_string(),
                            )
                        })?;
                let tag_names = Arc::clone(
                    &union_type
                        .value()
                        .union_def()
                        .expect("resolved union type has a union definition")
                        .tag_names,
                );
                Ok(Some((
                    hir::CustomTypeOperation::UnionTag {
                        union_type,
                        tag_names,
                    },
                    hir::TypeFact::known(Type::Text),
                )))
            }
            ScalarFunc::UnionExtractFunc => {
                let tag_name = string_literal_argument(
                    &arguments[1],
                    "union_extract() second argument must be a string literal",
                )?;
                let union_type =
                    custom_argument_type(&arguments[0], |definition| definition.is_union())
                        .ok_or_else(|| {
                            LimboError::ParseError(
                                "union_extract() first argument must have a known union type"
                                    .to_string(),
                            )
                        })?;
                let (tag_index, result_name) = union_type
                    .value()
                    .find_union_variant(&tag_name)
                    .map(|(index, variant)| (index, variant.type_name.clone()))
                    .ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "unknown variant '{}' in union type '{}'",
                            tag_name,
                            union_type.value().name
                        ))
                    })?;
                let result_type = self.resolve_named_type_fact(&result_name)?;
                Ok(Some((
                    hir::CustomTypeOperation::UnionExtract {
                        union_type,
                        tag_index,
                    },
                    result_type,
                )))
            }
            ScalarFunc::StructExtractFunc => {
                let field_name = string_literal_argument(
                    &arguments[1],
                    "struct_extract() second argument must be a string literal",
                )?;
                let struct_type =
                    custom_argument_type(&arguments[0], |definition| definition.is_struct())
                        .ok_or_else(|| {
                            LimboError::ParseError(
                                "struct_extract() first argument must have a known struct type"
                                    .to_string(),
                            )
                        })?;
                let (field_index, result_name) = struct_type
                    .value()
                    .find_struct_field(&field_name)
                    .map(|(index, field)| (index, field.type_name.clone()))
                    .ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "unknown field '{}' in struct type '{}'",
                            field_name,
                            struct_type.value().name
                        ))
                    })?;
                let result_type = self.resolve_named_type_fact(&result_name)?;
                Ok(Some((
                    hir::CustomTypeOperation::StructExtract {
                        struct_type,
                        field_index,
                    },
                    result_type,
                )))
            }
            _ => unreachable!("custom-type read function match is exhaustive"),
        }
    }

    fn resolve_sequence_operation(
        &mut self,
        function: &Func,
        input: &FunctionInput,
    ) -> Result<Option<(hir::FunctionOperation, hir::TypeFact)>> {
        let kind = match function {
            Func::Scalar(ScalarFunc::NextVal) => hir::SequenceOperationKind::NextValue,
            Func::Scalar(ScalarFunc::SetVal) => hir::SequenceOperationKind::SetValue,
            _ => return Ok(None),
        };
        let user_name =
            string_literal_argument(&input.facts()[0], "expected a string literal argument")?;
        let (database, normalized_name) = match user_name.split_once('.') {
            Some((schema, name)) => {
                let schema = normalize_ident(schema);
                let database = self.context().database(&schema).ok_or_else(|| {
                    LimboError::InvalidArgument(format!("no such database: {schema}"))
                })?;
                (database, normalize_ident(name))
            }
            None => (
                hir::DatabaseId::new(crate::MAIN_DB_ID),
                normalize_ident(&user_name),
            ),
        };
        let backing_table_name =
            crate::translate::sequence::sequence_backing_table_name(&normalized_name);
        let (backing_table, sequence, sqlite_sequence) = {
            let schema = self.context().main_schema();
            let backing_table = schema.get_table(&backing_table_name).ok_or_else(|| {
                LimboError::ParseError(format!("sequence \"{user_name}\" does not exist"))
            })?;
            let sequence = schema
                .get_sequence(&normalized_name)
                .cloned()
                .ok_or_else(|| {
                    LimboError::ParseError(format!("sequence \"{user_name}\" does not exist"))
                })?;
            let sqlite_sequence = normalized_name
                .starts_with(AUTOINCREMENT_SEQ_PREFIX)
                .then(|| schema.get_table(SQLITE_SEQUENCE_TABLE_NAME))
                .flatten();
            (backing_table, sequence, sqlite_sequence)
        };
        let backing_table_id =
            self.catalog_object_id(Some(database), CatalogObjectKind::Table, backing_table_name);
        let backing_table = hir::CatalogObject::new(
            backing_table_id,
            self.context().snapshot(),
            Some(database),
            backing_table,
        );
        let sequence_id = self.catalog_object_id(
            Some(database),
            CatalogObjectKind::Sequence,
            normalized_name.clone(),
        );
        let sequence = hir::CatalogObject::new(
            sequence_id,
            self.context().snapshot(),
            Some(database),
            sequence,
        );
        let sqlite_sequence = sqlite_sequence.map(|table| {
            let id = self.catalog_object_id(
                Some(database),
                CatalogObjectKind::Table,
                SQLITE_SEQUENCE_TABLE_NAME,
            );
            hir::CatalogObject::new(id, self.context().snapshot(), Some(database), table)
        });
        Ok(Some((
            hir::FunctionOperation::Sequence(hir::SequenceOperation {
                kind,
                user_name,
                normalized_name,
                sequence,
                backing_table,
                sqlite_sequence,
            }),
            hir::TypeFact::known(Type::Integer),
        )))
    }

    fn resolve_named_type_fact(&mut self, name: &str) -> Result<hir::TypeFact> {
        if self.context().custom_types_enabled() {
            if let Some(resolved) = self.context().main_schema().resolve_type_unchecked(name)? {
                return Ok(self.freeze_type_fact(name, resolved).0);
            }
        }
        let affinity = Affinity::affinity(name);
        Ok(hir::TypeFact::declared(hir::DeclaredType {
            name: name.to_string(),
            storage: affinity.to_type(),
            custom_chain: Vec::new(),
            array_dimensions: 0,
        }))
    }

    pub(super) fn resolve_atomic_expr(
        &self,
        expression: hir::Expr,
        scope: &Scope,
    ) -> Result<ResolvedScopeExpr> {
        match expression {
            hir::Expr::Literal(literal) => Ok(ResolvedScopeExpr {
                type_fact: super::analyze::literal_type_fact(&literal)?,
                expr: hir::Expr::Literal(literal),
                affinity: Affinity::Blob,
                has_affinity: false,
                collation: ExprCollation::Absent,
            }),
            hir::Expr::Parameter(parameter) => Ok(ResolvedScopeExpr {
                type_fact: parameter.type_fact.clone(),
                expr: hir::Expr::Parameter(parameter),
                affinity: Affinity::Blob,
                has_affinity: false,
                collation: ExprCollation::Absent,
            }),
            hir::Expr::Column(reference) => {
                let source = self.source(reference.source).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing semantic source {}",
                        reference.source
                    ))
                })?;
                let column = source.columns.get(reference.column).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing semantic column {}.{}",
                        reference.source, reference.column
                    ))
                })?;
                for state in [
                    &source.generated_expressions[reference.column],
                    &source.default_expressions[reference.column],
                ] {
                    if matches!(state, hir::ColumnReadExpression::NotRequired) {
                        return super::analyze::unsupported_select();
                    }
                }
                Ok(ResolvedScopeExpr {
                    expr: hir::Expr::Column(reference),
                    type_fact: column.type_fact.clone(),
                    affinity: column.affinity,
                    has_affinity: column.has_affinity,
                    collation: ExprCollation::inherited(column.collation.clone()),
                })
            }
            hir::Expr::MergedColumn(merged) => Ok(ResolvedScopeExpr {
                type_fact: merged.type_fact.clone(),
                affinity: merged.affinity,
                has_affinity: merged.has_affinity,
                collation: ExprCollation::inherited(merged.collation.clone()),
                expr: hir::Expr::MergedColumn(merged),
            }),
            hir::Expr::RowId(source) => Ok(ResolvedScopeExpr {
                expr: hir::Expr::RowId(source),
                type_fact: hir::TypeFact::known(Type::Integer),
                affinity: Affinity::Integer,
                has_affinity: true,
                collation: ExprCollation::Absent,
            }),
            hir::Expr::Output(output) => {
                let type_fact = scope.output_type(output).cloned().ok_or_else(|| {
                    LimboError::InternalError(format!("missing output facts for {output:?}"))
                })?;
                let affinity = scope.output_affinity(output).ok_or_else(|| {
                    LimboError::InternalError(format!("missing output affinity for {output:?}"))
                })?;
                let has_affinity = scope.output_has_affinity(output).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing output affinity state for {output:?}"
                    ))
                })?;
                let collation = scope.output_collation(output).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing output collation state for {output:?}"
                    ))
                })?;
                Ok(ResolvedScopeExpr {
                    expr: hir::Expr::Output(output),
                    type_fact,
                    affinity,
                    has_affinity,
                    collation,
                })
            }
            _ => super::analyze::unsupported_select(),
        }
    }

    pub(super) fn resolve_collation(&mut self, name: &ast::Name) -> Result<hir::ResolvedCollation> {
        let collation = match self.context().symbols().resolve_collation(name.as_str()) {
            Some(collation) => collation,
            None => CollationSeq::new(name.as_str())?,
        };
        let id = self.catalog_object_id(None, CatalogObjectKind::Collation, collation.to_string());
        Ok(hir::CatalogObject::new(
            id,
            self.context().snapshot(),
            None,
            Arc::new(collation),
        ))
    }

    fn resolve_cast_target(
        &mut self,
        syntax: Option<&ast::Type>,
        parameters: Vec<ResolvedScopeExpr>,
    ) -> Result<hir::TypeName> {
        let parameter_expressions = parameters
            .iter()
            .map(|parameter| parameter.expr.clone())
            .collect();
        let Some(syntax) = syntax else {
            return Ok(hir::TypeName {
                name: String::new(),
                parameters: parameter_expressions,
                array_dimensions: 0,
                type_fact: hir::TypeFact::dynamic(),
                affinity: Affinity::Numeric,
                programs: builtin_cast_programs(),
            });
        };
        if self.context().custom_types_enabled() {
            if let Some(resolved) = self
                .context()
                .main_schema()
                .resolve_type_unchecked(&syntax.name)?
            {
                if syntax.array_dimensions > 0
                    || resolved.leaf().user_params().count() != parameters.len()
                {
                    return super::analyze::unsupported_select();
                }
                let (type_fact, affinity) = self.freeze_type_fact(&syntax.name, resolved);
                let custom_chain = type_fact
                    .declared
                    .as_ref()
                    .expect("resolved custom type has a declaration")
                    .custom_chain
                    .clone();
                let mut encode = Vec::new();
                for definition in &custom_chain {
                    if let Some(call) = self.bind_type_encoder(definition, &parameters)? {
                        encode.push(call);
                    }
                }
                let domain = custom_chain
                    .first()
                    .filter(|definition| definition.value().is_domain)
                    .map(|_| self.bind_domain_constraints(&type_fact, &custom_chain))
                    .transpose()?;
                return Ok(hir::TypeName {
                    name: syntax.name.clone(),
                    parameters: parameter_expressions,
                    array_dimensions: 0,
                    type_fact,
                    affinity,
                    programs: hir::BoundCastPrograms {
                        encode,
                        domain,
                        apply_builtin_affinity: false,
                    },
                });
            }
        }
        let affinity = Affinity::affinity(&syntax.name);
        let type_fact = hir::TypeFact::declared(hir::DeclaredType {
            name: syntax.name.clone(),
            storage: affinity.to_type(),
            custom_chain: Vec::new(),
            array_dimensions: syntax.array_dimensions,
        });
        Ok(hir::TypeName {
            name: syntax.name.clone(),
            parameters: parameter_expressions,
            array_dimensions: syntax.array_dimensions,
            type_fact,
            affinity,
            programs: builtin_cast_programs(),
        })
    }

    fn freeze_type_fact(
        &mut self,
        name: &str,
        resolved: crate::schema::ResolvedType,
    ) -> (hir::TypeFact, Affinity) {
        let database = hir::DatabaseId::new(crate::MAIN_DB_ID);
        let affinity = Affinity::affinity(&resolved.primitive);
        let custom_chain = resolved
            .chain
            .into_iter()
            .map(|definition| {
                let id = self.catalog_object_id(
                    Some(database),
                    CatalogObjectKind::Type,
                    crate::util::normalize_ident(&definition.name),
                );
                hir::CatalogObject::new(id, self.context().snapshot(), Some(database), definition)
            })
            .collect();
        (
            hir::TypeFact::declared(hir::DeclaredType {
                name: name.to_string(),
                storage: affinity.to_type(),
                custom_chain,
                array_dimensions: 0,
            }),
            affinity,
        )
    }
}

fn like_lhs_count(lhs: &ast::Expr) -> usize {
    match lhs {
        ast::Expr::Parenthesized(expressions) if expressions.len() > 1 => expressions.len(),
        _ => 1,
    }
}

fn custom_argument_type(
    argument: &ResolvedScopeExpr,
    expected: impl FnOnce(&crate::schema::TypeDef) -> bool,
) -> Option<hir::ResolvedType> {
    argument
        .type_fact
        .declared
        .as_ref()?
        .custom()
        .filter(|definition| expected(definition.value()))
        .cloned()
}

fn string_literal_argument(argument: &ResolvedScopeExpr, error: &str) -> Result<String> {
    match &argument.expr {
        hir::Expr::Literal(ast::Literal::String(value)) => Ok(value.trim_matches('\'').to_string()),
        _ => Err(LimboError::ParseError(error.to_string())),
    }
}

fn validate_special_function_syntax(syntax: &ast::Expr) -> Result<()> {
    let ast::Expr::FunctionCall {
        name,
        args,
        order_by,
        within_group,
        filter_over,
        ..
    } = syntax
    else {
        return Ok(());
    };
    let function = normalize_ident(name.as_str());
    let valid_sequence_arity = match function.as_str() {
        "nextval" => args.len() == 1,
        "setval" => matches!(args.len(), 2 | 3),
        _ => false,
    };
    if valid_sequence_arity {
        if !matches!(
            args[0].as_ref(),
            ast::Expr::Literal(ast::Literal::String(_))
        ) {
            crate::bail_parse_error!("expected a string literal argument");
        }
        return Ok(());
    }
    let expected_arguments = match function.as_str() {
        "union_tag" => 1,
        "union_extract" | "struct_extract" => 2,
        _ => return Ok(()),
    };
    if args.len() != expected_arguments {
        crate::bail_parse_error!(
            "{}() requires exactly {} argument{}",
            function,
            expected_arguments,
            if expected_arguments == 1 { "" } else { "s" }
        );
    }
    if !order_by.is_empty() || !within_group.is_empty() {
        crate::bail_parse_error!("ORDER BY is not allowed for scalar function {}()", function);
    }
    if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() {
        crate::bail_parse_error!(
            "{}() may not be used as an aggregate or window function",
            function
        );
    }
    if matches!(function.as_str(), "union_extract" | "struct_extract")
        && !matches!(
            args[1].as_ref(),
            ast::Expr::Literal(ast::Literal::String(_))
        )
    {
        crate::bail_parse_error!("{}() second argument must be a string literal", function);
    }
    Ok(())
}

fn cast_parameter_count(type_name: Option<&ast::Type>) -> usize {
    match type_name.and_then(|type_name| type_name.size.as_ref()) {
        Some(ast::TypeSize::MaxSize(_)) => 1,
        Some(ast::TypeSize::TypeSize(_, _)) => 2,
        None => 0,
    }
}

fn builtin_cast_programs() -> hir::BoundCastPrograms {
    hir::BoundCastPrograms {
        encode: Vec::new(),
        domain: None,
        apply_builtin_affinity: true,
    }
}

fn expect_no_expr_children(children: ExprChildren) -> Result<()> {
    if children.is_empty() {
        Ok(())
    } else {
        Err(LimboError::InternalError(format!(
            "leaf expression produced {} child values",
            children.len()
        )))
    }
}

fn expect_expr_children<const N: usize>(children: ExprChildren) -> Result<[ResolvedScopeExpr; N]> {
    if children.len() != N {
        return Err(LimboError::InternalError(format!(
            "expression expected {N} child values, got {}",
            children.len()
        )));
    }
    let mut children = children.into_iter();
    Ok(std::array::from_fn(|_| {
        children.next().expect("expression child count was checked")
    }))
}

fn null_test_expr(inner: ResolvedScopeExpr, is_null: bool) -> ResolvedScopeExpr {
    let expression = if is_null {
        hir::Expr::IsNull(Box::new(inner.expr))
    } else {
        hir::Expr::NotNull(Box::new(inner.expr))
    };
    computed_expr(
        expression,
        hir::TypeFact::known(Type::Integer),
        inner.collation,
    )
}

fn computed_expr(
    expr: hir::Expr,
    type_fact: hir::TypeFact,
    collation: ExprCollation,
) -> ResolvedScopeExpr {
    ResolvedScopeExpr {
        expr,
        type_fact,
        affinity: Affinity::Blob,
        has_affinity: false,
        collation,
    }
}

fn effective_window_frame(
    frame: Option<FunctionWindowFrame>,
    order_by_len: usize,
    function: &Func,
) -> Result<hir::WindowFrame> {
    let Some(frame) = frame else {
        return Ok(hir::WindowFrame {
            mode: ast::FrameMode::Range,
            start: hir::WindowFrameBound::UnboundedPreceding,
            end: Some(hir::WindowFrameBound::CurrentRow),
            exclude: None,
        });
    };
    let end = frame.end.unwrap_or(hir::WindowFrameBound::CurrentRow);
    let illegal = matches!(
        (&frame.start, &end),
        (hir::WindowFrameBound::UnboundedFollowing, _)
            | (_, hir::WindowFrameBound::UnboundedPreceding)
            | (
                hir::WindowFrameBound::CurrentRow,
                hir::WindowFrameBound::Preceding(_)
            )
            | (
                hir::WindowFrameBound::Following(_),
                hir::WindowFrameBound::Preceding(_) | hir::WindowFrameBound::CurrentRow
            )
    );
    if illegal {
        crate::bail_parse_error!("unsupported frame specification");
    }
    if frame.mode == ast::FrameMode::Range
        && (matches!(
            &frame.start,
            hir::WindowFrameBound::Preceding(_) | hir::WindowFrameBound::Following(_)
        ) || matches!(
            &end,
            hir::WindowFrameBound::Preceding(_) | hir::WindowFrameBound::Following(_)
        ))
        && order_by_len != 1
    {
        crate::bail_parse_error!(
            "RANGE with offset PRECEDING/FOLLOWING requires one ORDER BY expression"
        );
    }
    let moving_start = !matches!(&frame.start, hir::WindowFrameBound::UnboundedPreceding);
    if moving_start && frame.exclude.is_none() && !function_supports_sliding_frame(function) {
        crate::bail_parse_error!(
            "{}() does not yet support window frames with a moving start; use a frame with UNBOUNDED PRECEDING start",
            function
        );
    }
    Ok(hir::WindowFrame {
        mode: frame.mode,
        start: frame.start,
        end: Some(end),
        exclude: frame.exclude,
    })
}

fn function_supports_sliding_frame(function: &Func) -> bool {
    let supported = matches!(
        function,
        Func::Agg(
            AggFunc::Sum
                | AggFunc::Total
                | AggFunc::Count
                | AggFunc::Count0
                | AggFunc::Avg
                | AggFunc::Min
                | AggFunc::Max
                | AggFunc::GroupConcat
                | AggFunc::StringAgg
        ) | Func::Window(WindowFunc::FirstValue | WindowFunc::NthValue | WindowFunc::LastValue)
    );
    #[cfg(feature = "json")]
    let supported = supported
        || matches!(
            function,
            Func::Agg(
                AggFunc::JsonGroupObject
                    | AggFunc::JsonbGroupObject
                    | AggFunc::JsonGroupArray
                    | AggFunc::JsonbGroupArray
            )
        );
    supported
}

fn coerced_window_frame(function: &WindowFunc) -> Option<hir::WindowFrame> {
    use hir::WindowFrameBound::{CurrentRow, Following, UnboundedFollowing, UnboundedPreceding};
    let (mode, start, end) = match function {
        WindowFunc::RowNumber | WindowFunc::Lag => {
            (ast::FrameMode::Rows, UnboundedPreceding, CurrentRow)
        }
        WindowFunc::Rank | WindowFunc::DenseRank => {
            (ast::FrameMode::Range, UnboundedPreceding, CurrentRow)
        }
        WindowFunc::PercentRank => (ast::FrameMode::Groups, CurrentRow, UnboundedFollowing),
        WindowFunc::CumeDist => (
            ast::FrameMode::Groups,
            Following(Box::new(hir::Expr::Literal(ast::Literal::Numeric(
                "1".to_string(),
            )))),
            UnboundedFollowing,
        ),
        WindowFunc::Ntile => (ast::FrameMode::Rows, CurrentRow, UnboundedFollowing),
        WindowFunc::Lead => (ast::FrameMode::Rows, UnboundedPreceding, UnboundedFollowing),
        WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue => return None,
        WindowFunc::External(_) => {
            unreachable!("WindowFunc::External is not constructible: ExtFunc has no Window variant")
        }
    };
    Some(hir::WindowFrame {
        mode,
        start,
        end: Some(end),
        exclude: None,
    })
}

fn bind_function(
    function: &Func,
    has_over: bool,
    name: &ast::Name,
    policy: ExprPolicy,
    context: &mut FunctionContext<'_>,
) -> Result<FunctionBinding> {
    let aggregate = matches!(function, Func::Agg(_))
        || matches!(function, Func::External(external) if external.func.is_aggregate());
    if aggregate && !has_over {
        match policy.aggregates {
            AggregatePolicy::Allow => {}
            AggregatePolicy::RejectFunction => {
                crate::bail_parse_error!("misuse of aggregate function {}()", function)
            }
            AggregatePolicy::RejectMisuse => {
                crate::bail_parse_error!("misuse of aggregate: {}()", function)
            }
        }
    }
    if has_over && !policy.allow_windows {
        match function {
            Func::Agg(_) | Func::Window(_) => {
                crate::bail_parse_error!("misuse of window function: {}()", function)
            }
            Func::External(external) if external.func.is_aggregate() => {
                crate::bail_parse_error!("misuse of window function: {}()", function)
            }
            _ => {
                crate::bail_parse_error!("{} may not be used as a window function", name.as_str())
            }
        }
    }
    match function {
        Func::Agg(_) if has_over => bind_window(function, context),
        Func::Agg(_) => bind_aggregate(function, context),
        Func::External(external) if external.func.is_aggregate() => {
            if has_over {
                bind_window(function, context)
            } else {
                bind_aggregate(function, context)
            }
        }
        Func::Window(_) if has_over => bind_window(function, context),
        Func::Window(window) => {
            crate::bail_parse_error!("misuse of window function: {}()", window)
        }
        _ if has_over => {
            crate::bail_parse_error!("{} may not be used as a window function", name.as_str())
        }
        Func::AlterTable(_) => super::analyze::unsupported_select(),
        _ => Ok(FunctionBinding::Scalar),
    }
}

fn bind_window(function: &Func, context: &mut FunctionContext<'_>) -> Result<FunctionBinding> {
    match context {
        FunctionContext::ScalarOnly => {
            crate::bail_parse_error!("misuse of window function: {}()", function)
        }
        FunctionContext::Query(state) => Ok(FunctionBinding::Window(WindowBinding {
            id: state.allocate_window(),
        })),
    }
}

fn bind_aggregate(function: &Func, context: &mut FunctionContext<'_>) -> Result<FunctionBinding> {
    match context {
        FunctionContext::ScalarOnly => {
            crate::bail_parse_error!("misuse of aggregate function {}()", function)
        }
        FunctionContext::Query(state) => Ok(FunctionBinding::Aggregate(AggregateBinding {
            id: state.allocate_aggregate(),
        })),
    }
}

fn resolve_ordered_set_function(
    name: &ast::Name,
    args: &[Box<ast::Expr>],
    distinctness: Option<&ast::Distinctness>,
    argument_order: &[ast::SortedColumn],
    within_group: &[ast::SortedColumn],
    tail: &ast::FunctionTail,
) -> Result<AggFunc> {
    let function = match normalize_ident(name.as_str()).as_str() {
        "mode" => AggFunc::Mode,
        "percentile_cont" => AggFunc::PercentileCont,
        "percentile_disc" => AggFunc::PercentileDisc,
        _ => {
            crate::bail_parse_error!(
                "WITHIN GROUP is not supported for function {}()",
                name.as_str()
            )
        }
    };
    if tail.over_clause.is_some() {
        crate::bail_parse_error!(
            "ordered-set aggregate {}() may not be used as a window function",
            name.as_str()
        );
    }
    if distinctness.is_some() {
        crate::bail_parse_error!(
            "DISTINCT is not supported for ordered-set aggregate {}()",
            name.as_str()
        );
    }
    if !argument_order.is_empty() {
        crate::bail_parse_error!(
            "{}() does not accept an argument ORDER BY together with WITHIN GROUP",
            name.as_str()
        );
    }
    if within_group.len() != 1 {
        crate::bail_parse_error!(
            "WITHIN GROUP for {}() must specify exactly one ORDER BY expression",
            name.as_str()
        );
    }
    let order_by = &within_group[0];
    if matches!(order_by.order, Some(ast::SortOrder::Desc)) || order_by.nulls.is_some() {
        crate::bail_parse_error!(
            "DESC and NULLS ordering inside WITHIN GROUP are not supported yet"
        );
    }
    let expected_direct = match function {
        AggFunc::Mode => 0,
        AggFunc::PercentileCont | AggFunc::PercentileDisc => 1,
        _ => unreachable!("ordered-set function list is exhaustive"),
    };
    if args.len() != expected_direct {
        crate::bail_parse_error!("wrong number of arguments to function {}()", name.as_str());
    }
    Ok(function)
}

fn nested_function_iter<'a>(
    arguments: impl IntoIterator<Item = &'a ResolvedScopeExpr>,
) -> Option<NestedFunction> {
    arguments
        .into_iter()
        .find_map(|argument| nested_function_in_expr(&argument.expr))
}

fn nested_function_in_expr(expression: &hir::Expr) -> Option<NestedFunction> {
    let mut nested = None;
    expression.walk(&mut |expression| {
        if nested.is_some() {
            return;
        }
        let hir::Expr::Function(call) = expression else {
            return;
        };
        nested = match call.evaluation {
            hir::FunctionEvaluation::Aggregate { .. } => {
                Some(NestedFunction::Aggregate(call.function.value().to_string()))
            }
            hir::FunctionEvaluation::Window { .. } => {
                Some(NestedFunction::Window(call.function.value().to_string()))
            }
            hir::FunctionEvaluation::Scalar => None,
        };
    });
    nested
}

fn nested_function_in_window_bound(bound: &hir::WindowFrameBound) -> Option<NestedFunction> {
    match bound {
        hir::WindowFrameBound::Following(expression)
        | hir::WindowFrameBound::Preceding(expression) => nested_function_in_expr(expression),
        hir::WindowFrameBound::CurrentRow
        | hir::WindowFrameBound::UnboundedFollowing
        | hir::WindowFrameBound::UnboundedPreceding => None,
    }
}

fn validate_raise(action: ast::ResolveType, policy: RaisePolicy) -> Result<()> {
    match (policy, action) {
        (RaisePolicy::AbortOnly, ast::ResolveType::Abort) | (RaisePolicy::Trigger, _) => Ok(()),
        (RaisePolicy::AbortOnly, _) => {
            crate::bail_parse_error!("RAISE() may only be used within a trigger-program")
        }
    }
}

fn function_result_type(function: &Func, arguments: &[ResolvedScopeExpr]) -> hir::TypeFact {
    match function {
        Func::Scalar(
            ScalarFunc::Length | ScalarFunc::NextVal | ScalarFunc::CurrVal | ScalarFunc::SetVal,
        ) => hir::TypeFact::known(Type::Integer),
        Func::Scalar(ScalarFunc::Abs) => {
            arguments
                .first()
                .map_or_else(hir::TypeFact::dynamic, |argument| {
                    match argument.type_fact.storage {
                        Some(Type::Null) => hir::TypeFact::known(Type::Null),
                        Some(Type::Integer) => hir::TypeFact::known(Type::Integer),
                        Some(Type::Real) | Some(Type::Text) | Some(Type::Blob) => {
                            hir::TypeFact::known(Type::Real)
                        }
                        Some(Type::Numeric) => hir::TypeFact::known(Type::Numeric),
                        None => hir::TypeFact::dynamic(),
                    }
                })
        }
        Func::Agg(AggFunc::Count | AggFunc::Count0) => hir::TypeFact::known(Type::Integer),
        Func::Agg(AggFunc::Avg | AggFunc::Total | AggFunc::PercentileCont) => {
            hir::TypeFact::known(Type::Real)
        }
        Func::Agg(AggFunc::Sum) => hir::TypeFact::known(Type::Numeric),
        Func::Agg(AggFunc::Min | AggFunc::Max | AggFunc::Mode | AggFunc::PercentileDisc) => {
            arguments
                .first()
                .map_or_else(hir::TypeFact::dynamic, |argument| {
                    argument.type_fact.clone()
                })
        }
        Func::Agg(AggFunc::GroupConcat | AggFunc::StringAgg) => hir::TypeFact::known(Type::Text),
        Func::Agg(AggFunc::ArrayAgg) => arguments.first().map_or_else(
            || hir::TypeFact::known_array(1),
            |argument| hir::TypeFact::array_literal_result([argument.type_fact.clone()]),
        ),
        Func::Window(
            WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank | WindowFunc::Ntile,
        ) => hir::TypeFact::known(Type::Integer),
        Func::Window(WindowFunc::PercentRank | WindowFunc::CumeDist) => {
            hir::TypeFact::known(Type::Real)
        }
        Func::Window(
            WindowFunc::Lag
            | WindowFunc::Lead
            | WindowFunc::FirstValue
            | WindowFunc::LastValue
            | WindowFunc::NthValue,
        ) => arguments
            .first()
            .map_or_else(hir::TypeFact::dynamic, |argument| {
                argument.type_fact.clone()
            }),
        _ => hir::TypeFact::dynamic(),
    }
}

fn expression_collation(lhs: &ExprCollation, rhs: &ExprCollation) -> ExprCollation {
    match (lhs, rhs) {
        (ExprCollation::Explicit(collation), _) => ExprCollation::Explicit(collation.clone()),
        (_, ExprCollation::Explicit(collation)) => ExprCollation::Explicit(collation.clone()),
        (ExprCollation::Inherited(collation), _) => ExprCollation::Inherited(collation.clone()),
        (_, ExprCollation::Inherited(collation)) => ExprCollation::Inherited(collation.clone()),
        (ExprCollation::Absent, ExprCollation::Absent) => ExprCollation::Absent,
    }
}

fn binary_type_fact(
    operator: ast::Operator,
    lhs: &hir::TypeFact,
    rhs: &hir::TypeFact,
) -> hir::TypeFact {
    use ast::Operator;

    match operator {
        Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
            hir::TypeFact::arithmetic_result(lhs, rhs)
        }
        Operator::Concat => hir::TypeFact::concat_result(lhs, rhs),
        Operator::ArrowRight | Operator::ArrowRightShift => hir::TypeFact::dynamic(),
        Operator::Modulus
        | Operator::And
        | Operator::Or
        | Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseNot
        | Operator::LeftShift
        | Operator::RightShift
        | Operator::Equals
        | Operator::NotEquals
        | Operator::Less
        | Operator::LessEquals
        | Operator::Greater
        | Operator::GreaterEquals
        | Operator::Is
        | Operator::IsNot
        | Operator::ArrayContains
        | Operator::ArrayOverlap => hir::TypeFact::known(Type::Integer),
    }
}

fn comparison_semantics(
    lhs: &ResolvedScopeExpr,
    rhs: &ResolvedScopeExpr,
) -> hir::ComparisonSemantics {
    hir::ComparisonSemantics {
        components: vec![comparison_component(lhs, rhs)],
    }
}

fn comparison_component(
    lhs: &ResolvedScopeExpr,
    rhs: &ResolvedScopeExpr,
) -> hir::ComparisonComponent {
    let affinity = match (lhs.has_affinity, rhs.has_affinity) {
        (true, true) if lhs.affinity.is_numeric() || rhs.affinity.is_numeric() => Affinity::Numeric,
        (true, true) => Affinity::Blob,
        (true, false) => lhs.affinity,
        (false, true) => rhs.affinity,
        (false, false) => Affinity::Blob,
    };
    hir::ComparisonComponent {
        affinity,
        collation: expression_collation(&lhs.collation, &rhs.collation)
            .value()
            .cloned(),
        array: lhs.type_fact.is_array() && rhs.type_fact.is_array(),
    }
}

pub(super) fn build_using_column(
    name: String,
    left: ResolvedScopeExpr,
    right: ResolvedScopeExpr,
    value: hir::MergedColumnValue,
) -> Result<hir::UsingColumn> {
    let hir::Expr::Column(right_reference) = &right.expr else {
        return Err(LimboError::InternalError(
            "USING right side is not a source column".to_string(),
        ));
    };
    let right_reference = *right_reference;
    let comparison = comparison_semantics(&left, &right);
    let (type_fact, affinity, has_affinity, collation) = match value {
        hir::MergedColumnValue::Left => (
            left.type_fact.clone(),
            left.affinity,
            left.has_affinity,
            left.collation.value().cloned(),
        ),
        hir::MergedColumnValue::Right => (
            right.type_fact.clone(),
            right.affinity,
            right.has_affinity,
            right.collation.value().cloned(),
        ),
        hir::MergedColumnValue::Coalesce => (
            hir::TypeFact::selected_value_result([&left.type_fact, &right.type_fact]),
            Affinity::Blob,
            false,
            None,
        ),
    };

    Ok(hir::UsingColumn {
        name,
        left: Box::new(left.expr),
        right: right_reference,
        value,
        type_fact,
        affinity,
        has_affinity,
        collation,
        comparison,
    })
}

fn in_comparison_semantics(
    lhs: &ResolvedScopeExpr,
    rhs: &ResolvedScopeExpr,
) -> hir::ComparisonSemantics {
    hir::ComparisonSemantics {
        components: vec![hir::ComparisonComponent {
            affinity: lhs.affinity,
            collation: expression_collation(&lhs.collation, &rhs.collation)
                .value()
                .cloned(),
            array: lhs.type_fact.is_array() && rhs.type_fact.is_array(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dialect::SqliteDialect,
        schema::{Schema, Type},
        sync::Arc,
        translate::semantic::{
            context::{DoubleQuotedDml, SemanticContext},
            hir::{
                ColumnReadExpression, DatabaseId, Expr, IndexCoverage, IndexHint, Source,
                SourceColumn, SourceId, SourceKind, SourceOwner, TypeFact,
            },
            scope::Scope,
        },
        vdbe::affinity::Affinity,
        SymbolTable, MAIN_DB_ID,
    };
    use turso_parser::parser::Parser;

    fn expression(sql: &str) -> ast::Expr {
        let ast::Cmd::Stmt(ast::Stmt::Select(select)) = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains statement")
        else {
            panic!("SQL contains SELECT");
        };
        let ast::OneSelect::Select { columns, .. } = select.body.select else {
            panic!("SQL contains SELECT body");
        };
        let ast::ResultColumn::Expr(expression, _) = &columns[0] else {
            panic!("SELECT has expression output");
        };
        expression.as_ref().clone()
    }

    fn source() -> Source {
        Source {
            id: SourceId::new(0),
            owner: SourceOwner::Root,
            database: Some(DatabaseId::new(MAIN_DB_ID)),
            name: "items".to_string(),
            alias: None,
            kind: SourceKind::SchemaExpression,
            columns: vec![SourceColumn {
                name: "value".to_string(),
                type_fact: TypeFact::known(Type::Text),
                affinity: Affinity::Text,
                has_affinity: true,
                collation: None,
                hidden: false,
                rowid_alias: false,
            }],
            generated_expressions: vec![ColumnReadExpression::Absent],
            default_expressions: vec![ColumnReadExpression::Absent],
            column_type_programs: vec![None],
            check_constraints: None,
            rowid_available: true,
            index_hint: IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    fn analyze_expression(syntax: &ast::Expr, scope: &Scope, policy: ExprPolicy) -> Result<Expr> {
        let schema = Schema::new();
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            &schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        let mut analyzer = Analyzer::new(&context);
        let source_id = analyzer.reserve_source();
        analyzer.insert_source(source_id, source())?;
        analyzer.analyze_expr(syntax, scope, policy)
    }

    fn expect_column(expression: Expr) {
        assert!(matches!(
            expression,
            Expr::Column(column) if column.source == SourceId::new(0) && column.column == 0
        ));
    }

    #[test]
    fn empty_in_lists_keep_negation_without_comparisons() {
        for negated in [false, true] {
            let syntax = ast::Expr::InList {
                lhs: Box::new(expression("SELECT 1")),
                not: negated,
                rhs: Vec::new(),
            };
            let expression = analyze_expression(
                &syntax,
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("empty IN list binds");
            let Expr::InList {
                negated: actual,
                values,
                comparisons,
                ..
            } = expression
            else {
                panic!("empty IN list becomes HIR");
            };
            assert_eq!(actual, negated);
            assert!(values.is_empty());
            assert!(comparisons.is_empty());
        }
    }

    #[test]
    fn cast_parameters_bind_before_input_expression() {
        let syntax = ast::Expr::Cast {
            expr: Box::new(ast::Expr::Id(ast::Name::exact("missing_input".to_string()))),
            type_name: Some(ast::Type {
                name: "DECIMAL".to_string(),
                size: Some(ast::TypeSize::MaxSize(Box::new(ast::Expr::Id(
                    ast::Name::exact("missing_parameter".to_string()),
                )))),
                array_dimensions: 0,
            }),
        };
        let error = analyze_expression(
            &syntax,
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("first unresolved CAST child fails");
        assert_eq!(
            error.to_string(),
            "Parse error: no such column: missing_parameter"
        );
    }

    #[test]
    fn identifier_forms_become_hir_column_references() {
        let source = source();
        let mut scope = Scope::default();
        scope.add_source(&source, true);

        for sql in [
            "SELECT value",
            "SELECT items.value",
            "SELECT main.items.value",
        ] {
            expect_column(
                analyze_expression(
                    &expression(sql),
                    &scope,
                    ExprPolicy::select(DoubleQuotedDml::Enabled),
                )
                .expect("name resolves"),
            );
        }

        expect_column(
            analyze_expression(
                &ast::Expr::Name(ast::Name::exact("value".to_string())),
                &scope,
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("Name node follows identifier rules"),
        );
    }

    #[test]
    fn qualified_name_errors_keep_table_and_column_distinction() {
        let source = source();
        let mut scope = Scope::default();
        scope.add_source(&source, true);

        let missing_table = analyze_expression(
            &expression("SELECT absent.value"),
            &scope,
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("unknown qualifier fails");
        assert_eq!(
            missing_table.to_string(),
            "Parse error: no such table: absent"
        );

        let missing_column = analyze_expression(
            &expression("SELECT items.absent"),
            &scope,
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("known qualifier with unknown column fails");
        assert_eq!(
            missing_column.to_string(),
            "Parse error: no such column: items.absent"
        );
    }

    #[test]
    fn clause_policy_can_disable_dqs_fallback() {
        let error = analyze_expression(
            &expression("SELECT \"missing\""),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled).without_dqs_fallback(),
        )
        .expect_err("clause policy disables DQS");
        assert_eq!(error.to_string(), "Parse error: no such column: missing");
    }

    #[test]
    fn scalar_only_context_rejects_aggregate_functions() {
        let error = analyze_expression(
            &expression("SELECT sum(1)"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("schema-style expression context rejects aggregates");
        assert_eq!(
            error.to_string(),
            "Parse error: misuse of aggregate function sum()"
        );
    }

    #[test]
    fn like_family_becomes_resolved_hir() {
        for (sql, expected_operator, expected_negated, expected_arguments) in [
            (
                "SELECT 'alphabet' LIKE 'alpha%'",
                ast::LikeOperator::Like,
                false,
                2,
            ),
            (
                "SELECT 'alphabet' NOT GLOB 'z*'",
                ast::LikeOperator::Glob,
                true,
                2,
            ),
            (
                "SELECT 'a_b' LIKE 'a!_b' ESCAPE '!'",
                ast::LikeOperator::Like,
                false,
                3,
            ),
        ] {
            let analyzed = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("LIKE-family expression binds");
            let Expr::Like {
                operator,
                negated,
                argument_count,
                function,
                escape,
                ..
            } = analyzed
            else {
                panic!("LIKE-family syntax becomes LIKE HIR");
            };
            assert_eq!(operator, expected_operator);
            assert_eq!(negated, expected_negated);
            assert_eq!(argument_count, expected_arguments);
            assert_eq!(escape.is_some(), expected_arguments == 3);
            assert!(matches!(
                (operator, function.value()),
                (ast::LikeOperator::Like, Func::Scalar(ScalarFunc::Like))
                    | (ast::LikeOperator::Glob, Func::Scalar(ScalarFunc::Glob))
            ));
        }

        for operator in ["GLOB", "REGEXP", "MATCH"] {
            let error = analyze_expression(
                &expression(&format!("SELECT 'value' {operator} 'pattern' ESCAPE '!'")),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect_err("only LIKE accepts ESCAPE");
            assert_eq!(
                error.to_string(),
                format!("Parse error: wrong number of arguments to function {operator}()")
            );
        }

        let error = analyze_expression(
            &expression("SELECT 'value' REGEXP 'pattern'"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("REGEXP requires a registered function");
        assert_eq!(error.to_string(), "Parse error: no such function: regexp");
    }

    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    #[test]
    fn match_accepts_a_row_valued_left_side() {
        let analyzed = analyze_expression(
            &expression("SELECT ('one', 'two') MATCH 'query'"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect("MATCH accepts multiple document columns");
        let Expr::Like {
            lhs,
            operator: ast::LikeOperator::Match,
            function,
            argument_count: 3,
            ..
        } = analyzed
        else {
            panic!("MATCH becomes resolved LIKE-family HIR");
        };
        assert!(matches!(*lhs, Expr::Row(values) if values.len() == 2));
        assert!(matches!(
            function.value(),
            Func::Fts(crate::function::FtsFunc::Match)
        ));
    }

    #[cfg(any(not(feature = "fts"), target_family = "wasm"))]
    #[test]
    fn match_requires_fts_support() {
        let error = analyze_expression(
            &expression("SELECT 'document' MATCH 'query'"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("MATCH requires FTS support");
        assert_eq!(
            error.to_string(),
            "Parse error: MATCH requires the 'fts' feature to be enabled"
        );
    }

    #[test]
    fn deeply_nested_like_expressions_use_expression_frames() {
        const DEPTH: usize = 10_000;

        let mut syntax = ast::Expr::Literal(ast::Literal::String("value".to_string()));
        for _ in 0..DEPTH {
            syntax = ast::Expr::Like {
                lhs: Box::new(syntax),
                not: false,
                op: ast::LikeOperator::Like,
                rhs: Box::new(ast::Expr::Literal(ast::Literal::String("%".to_string()))),
                escape: None,
            };
        }

        let mut analyzed = analyze_expression(
            &syntax,
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect("deep LIKE expression binds without using the call stack");
        for _ in 0..DEPTH {
            let Expr::Like { lhs, .. } = analyzed else {
                panic!("expected nested LIKE HIR expression");
            };
            analyzed = *lhs;
        }
        assert!(matches!(
            analyzed,
            Expr::Literal(ast::Literal::String(value)) if value == "value"
        ));

        for _ in 0..DEPTH {
            let ast::Expr::Like { lhs, .. } = syntax else {
                panic!("expected nested LIKE parser expression");
            };
            syntax = *lhs;
        }
        assert!(matches!(
            syntax,
            ast::Expr::Literal(ast::Literal::String(value)) if value == "value"
        ));
    }

    #[test]
    fn deeply_nested_expressions_use_expression_frames() {
        const DEPTH: usize = 20_000;

        let mut syntax = ast::Expr::Literal(ast::Literal::Numeric("1".to_string()));
        for _ in 0..DEPTH {
            syntax = ast::Expr::Unary(ast::UnaryOperator::Positive, Box::new(syntax));
        }

        let mut analyzed = analyze_expression(
            &syntax,
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect("deep expression binds without using the call stack");

        for _ in 0..DEPTH {
            let Expr::Unary {
                operator: ast::UnaryOperator::Positive,
                expr,
            } = analyzed
            else {
                panic!("expected nested unary HIR expression");
            };
            analyzed = *expr;
        }
        assert!(matches!(
            analyzed,
            Expr::Literal(ast::Literal::Numeric(value)) if value == "1"
        ));

        // Take the parser tree apart iteratively too, so dropping the test input
        // does not hide analyzer behavior behind the recursive enum destructor.
        for _ in 0..DEPTH {
            let ast::Expr::Unary(ast::UnaryOperator::Positive, inner) = syntax else {
                panic!("expected nested unary parser expression");
            };
            syntax = *inner;
        }
        assert!(matches!(
            syntax,
            ast::Expr::Literal(ast::Literal::Numeric(value)) if value == "1"
        ));
    }
}
