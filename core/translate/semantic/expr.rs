//! Parser-expression conversion into resolved HIR expressions.

use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    context::DoubleQuotedDml,
    hir,
    scope::{ExprCollation, NamePrecedence, ResolvedScopeExpr, Scope},
};
use crate::{
    schema::Type, sync::Arc, translate::collate::CollationSeq, vdbe::affinity::Affinity,
    LimboError, Result,
};

/// Clause rules that change expression name visibility.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExprPolicy {
    precedence: NamePrecedence,
    allow_dqs_fallback: bool,
}

impl ExprPolicy {
    pub(crate) const fn select(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: dqs_dml.is_enabled(),
        }
    }

    pub(crate) const fn without_dqs_fallback(mut self) -> Self {
        self.allow_dqs_fallback = false;
        self
    }
}

enum ExprTask<'a> {
    Visit(&'a ast::Expr),
    BuildUnary(ast::UnaryOperator),
    BuildBinary(ast::Operator),
    BuildCollate(&'a ast::Name),
    BuildIsNull,
    BuildNotNull,
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
        let mut tasks = vec![ExprTask::Visit(syntax)];
        let mut values = Vec::new();

        while let Some(task) = tasks.pop() {
            match task {
                ExprTask::Visit(syntax) => match syntax {
                    ast::Expr::Literal(literal) => values.push(
                        self.resolve_atomic_expr(hir::Expr::Literal(literal.clone()), scope)?,
                    ),
                    ast::Expr::Id(name) | ast::Expr::Name(name) => {
                        let expression =
                            match scope.resolve_unqualified(name.as_str(), policy.precedence)? {
                                Some(resolved) => resolved.expr,
                                None if policy.allow_dqs_fallback && name.quoted_with('"') => {
                                    hir::Expr::Literal(ast::Literal::String(name.as_literal()))
                                }
                                None => {
                                    crate::bail_parse_error!("no such column: {}", name.as_str())
                                }
                            };
                        values.push(self.resolve_atomic_expr(expression, scope)?);
                    }
                    ast::Expr::Qualified(table, column) => {
                        let Some(resolved) =
                            scope.resolve_qualified(table.as_str(), column.as_str())?
                        else {
                            crate::bail_parse_error!("no such table: {}", table.as_str());
                        };
                        values.push(self.resolve_atomic_expr(resolved.expr, scope)?);
                    }
                    ast::Expr::DoublyQualified(database, table, column) => {
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
                        values.push(self.resolve_atomic_expr(resolved.expr, scope)?);
                    }
                    ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                        tasks.push(ExprTask::Visit(&expressions[0]));
                    }
                    ast::Expr::Variable(variable) => {
                        if variable.col_type.is_some() {
                            return super::analyze::unsupported_select();
                        }
                        values.push(self.resolve_atomic_expr(
                            hir::Expr::Parameter(hir::Parameter {
                                index: variable.index,
                                name: variable.name.as_deref().map(str::to_owned),
                                type_fact: hir::TypeFact::dynamic(),
                            }),
                            scope,
                        )?);
                    }
                    ast::Expr::Unary(operator, expression) => {
                        tasks.push(ExprTask::BuildUnary(*operator));
                        tasks.push(ExprTask::Visit(expression));
                    }
                    ast::Expr::Binary(expression, ast::Operator::Is, rhs)
                        if matches!(rhs.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
                    {
                        tasks.push(ExprTask::BuildIsNull);
                        tasks.push(ExprTask::Visit(expression));
                    }
                    ast::Expr::Binary(expression, ast::Operator::IsNot, rhs)
                        if matches!(rhs.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
                    {
                        tasks.push(ExprTask::BuildNotNull);
                        tasks.push(ExprTask::Visit(expression));
                    }
                    ast::Expr::Binary(lhs, ast::Operator::Is, expression)
                        if matches!(lhs.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
                    {
                        tasks.push(ExprTask::BuildIsNull);
                        tasks.push(ExprTask::Visit(expression));
                    }
                    ast::Expr::Binary(lhs, ast::Operator::IsNot, expression)
                        if matches!(lhs.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
                    {
                        tasks.push(ExprTask::BuildNotNull);
                        tasks.push(ExprTask::Visit(expression));
                    }
                    ast::Expr::Binary(lhs, operator, rhs) => {
                        tasks.push(ExprTask::BuildBinary(*operator));
                        tasks.push(ExprTask::Visit(rhs));
                        tasks.push(ExprTask::Visit(lhs));
                    }
                    ast::Expr::Collate(expression, name) => {
                        tasks.push(ExprTask::BuildCollate(name));
                        tasks.push(ExprTask::Visit(expression));
                    }
                    ast::Expr::IsNull(expression) => {
                        tasks.push(ExprTask::BuildIsNull);
                        tasks.push(ExprTask::Visit(expression));
                    }
                    ast::Expr::NotNull(expression) => {
                        tasks.push(ExprTask::BuildNotNull);
                        tasks.push(ExprTask::Visit(expression));
                    }
                    _ => return super::analyze::unsupported_select(),
                },
                ExprTask::BuildUnary(operator) => {
                    let inner = pop_expr_value(&mut values)?;
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
                    values.push(computed_expr(
                        hir::Expr::Unary {
                            operator,
                            expr: Box::new(inner.expr),
                        },
                        type_fact,
                        inner.collation,
                    ));
                }
                ExprTask::BuildBinary(operator) => {
                    let rhs = pop_expr_value(&mut values)?;
                    let lhs = pop_expr_value(&mut values)?;
                    let type_fact = binary_type_fact(operator, &lhs.type_fact, &rhs.type_fact);
                    let array_concat = operator == ast::Operator::Concat
                        && (lhs.type_fact.is_array() || rhs.type_fact.is_array());
                    let comparison = operator
                        .is_comparison()
                        .then(|| comparison_semantics(&lhs, &rhs));
                    let collation = expression_collation(&lhs.collation, &rhs.collation);
                    values.push(computed_expr(
                        hir::Expr::Binary {
                            lhs: Box::new(lhs.expr),
                            operator,
                            rhs: Box::new(rhs.expr),
                            array_concat,
                            custom: None,
                            comparison,
                        },
                        type_fact,
                        collation,
                    ));
                }
                ExprTask::BuildCollate(name) => {
                    let inner = pop_expr_value(&mut values)?;
                    let collation = self.resolve_collation(name)?;
                    values.push(ResolvedScopeExpr {
                        expr: hir::Expr::Collate {
                            expr: Box::new(inner.expr),
                            collation: collation.clone(),
                        },
                        type_fact: inner.type_fact,
                        affinity: inner.affinity,
                        has_affinity: inner.has_affinity,
                        collation: ExprCollation::Explicit(collation),
                    });
                }
                ExprTask::BuildIsNull | ExprTask::BuildNotNull => {
                    let inner = pop_expr_value(&mut values)?;
                    let expression = match task {
                        ExprTask::BuildIsNull => hir::Expr::IsNull(Box::new(inner.expr)),
                        ExprTask::BuildNotNull => hir::Expr::NotNull(Box::new(inner.expr)),
                        _ => unreachable!(),
                    };
                    values.push(computed_expr(
                        expression,
                        hir::TypeFact::known(Type::Integer),
                        inner.collation,
                    ));
                }
            }
        }

        if values.len() != 1 {
            return Err(LimboError::InternalError(format!(
                "expression work stack produced {} values",
                values.len()
            )));
        }
        Ok(values.pop().expect("expression value count was checked"))
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
}

fn pop_expr_value(values: &mut Vec<ResolvedScopeExpr>) -> Result<ResolvedScopeExpr> {
    values.pop().ok_or_else(|| {
        LimboError::InternalError("expression work stack is missing a value".to_string())
    })
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
    fn deeply_nested_expressions_use_an_explicit_work_stack() {
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
