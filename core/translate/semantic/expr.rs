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
    function::{Func, ScalarFunc},
    schema::Type,
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
    raise: RaisePolicy,
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
            raise: RaisePolicy::AbortOnly,
        }
    }
}

type ExprChildren = SmallVec<[ResolvedScopeExpr; 3]>;

struct ExprFrame<'a> {
    syntax: &'a ast::Expr,
    next_child: usize,
    resolved_children: ExprChildren,
}

impl<'a> ExprFrame<'a> {
    fn new(syntax: &'a ast::Expr) -> Self {
        Self {
            syntax,
            next_child: 0,
            resolved_children: SmallVec::new(),
        }
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
            ast::Expr::FunctionCall { args, .. } => args.get(self.next_child).map(Box::as_ref),
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

impl Analyzer<'_, '_> {
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
        let mut frames = vec![ExprFrame::new(syntax)];
        loop {
            if let Some(child) = frames
                .last_mut()
                .expect("root expression frame exists")
                .next_child()
            {
                frames.push(ExprFrame::new(child));
                continue;
            }

            let frame = frames.pop().expect("completed expression frame exists");
            let resolved = self.build_expr(frame.syntax, frame.resolved_children, scope, policy)?;
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
                if distinctness.is_some()
                    || !order_by.is_empty()
                    || !within_group.is_empty()
                    || filter_over.filter_clause.is_some()
                    || filter_over.over_clause.is_some()
                {
                    return super::analyze::unsupported_select();
                }
                if children.len() != args.len() {
                    return Err(LimboError::InternalError(format!(
                        "function expression expected {} child values, got {}",
                        args.len(),
                        children.len()
                    )));
                }
                let function_name = normalize_ident(name.as_str());
                let Some(function) = self
                    .context()
                    .resolve_function(&function_name, args.len())?
                else {
                    crate::bail_parse_error!("no such function: {function_name}");
                };
                if !is_scalar_function(&function) {
                    return super::analyze::unsupported_select();
                }
                let result_type = scalar_function_result_type(&function, &children);
                let id = self.catalog_object_id(
                    None,
                    CatalogObjectKind::Function {
                        argument_count: args.len(),
                    },
                    function_name,
                );
                let function = hir::CatalogObject::new(
                    id,
                    self.context().snapshot(),
                    None,
                    Arc::new(function),
                );
                Ok(computed_expr(
                    hir::Expr::Function(hir::FunctionCall {
                        function,
                        evaluation: hir::FunctionEvaluation::Scalar,
                        star: false,
                        arguments: children.into_iter().map(|child| child.expr).collect(),
                        distinctness: None,
                        argument_order: Vec::new(),
                        within_group: Vec::new(),
                        filter: None,
                        window: None,
                        result_type: result_type.clone(),
                        custom_type_operation: None,
                        sequence_operation: None,
                    }),
                    result_type,
                    ExprCollation::Absent,
                ))
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

    fn resolve_collation(&mut self, name: &ast::Name) -> Result<hir::ResolvedCollation> {
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
                let database = hir::DatabaseId::new(crate::MAIN_DB_ID);
                let storage = Affinity::affinity(&resolved.primitive).to_type();
                let affinity = Affinity::affinity(&resolved.primitive);
                let mut custom_chain = Vec::with_capacity(resolved.chain.len());
                for definition in resolved.chain {
                    let id = self.catalog_object_id(
                        Some(database),
                        CatalogObjectKind::Type,
                        crate::util::normalize_ident(&definition.name),
                    );
                    custom_chain.push(hir::CatalogObject::new(
                        id,
                        self.context().snapshot(),
                        Some(database),
                        definition,
                    ));
                }
                let mut encode = Vec::new();
                for definition in &custom_chain {
                    if let Some(call) = self.bind_type_encoder(definition, &parameters)? {
                        encode.push(call);
                    }
                }
                let type_fact = hir::TypeFact::declared(hir::DeclaredType {
                    name: syntax.name.clone(),
                    storage,
                    custom_chain: custom_chain.clone(),
                    array_dimensions: 0,
                });
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

fn is_scalar_function(function: &Func) -> bool {
    !matches!(
        function,
        Func::Agg(_) | Func::Window(_) | Func::AlterTable(_)
    )
}

fn validate_raise(action: ast::ResolveType, policy: RaisePolicy) -> Result<()> {
    match (policy, action) {
        (RaisePolicy::AbortOnly, ast::ResolveType::Abort) | (RaisePolicy::Trigger, _) => Ok(()),
        (RaisePolicy::AbortOnly, _) => {
            crate::bail_parse_error!("RAISE() may only be used within a trigger-program")
        }
    }
}

fn scalar_function_result_type(function: &Func, arguments: &[ResolvedScopeExpr]) -> hir::TypeFact {
    match function {
        Func::Scalar(ScalarFunc::Length) => hir::TypeFact::known(Type::Integer),
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
    let affinity = match (lhs.has_affinity, rhs.has_affinity) {
        (true, true) if lhs.affinity.is_numeric() || rhs.affinity.is_numeric() => Affinity::Numeric,
        (true, true) => Affinity::Blob,
        (true, false) => lhs.affinity,
        (false, true) => rhs.affinity,
        (false, false) => Affinity::Blob,
    };
    hir::ComparisonSemantics {
        components: vec![hir::ComparisonComponent {
            affinity,
            collation: expression_collation(&lhs.collation, &rhs.collation)
                .value()
                .cloned(),
            array: lhs.type_fact.is_array() && rhs.type_fact.is_array(),
        }],
    }
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
