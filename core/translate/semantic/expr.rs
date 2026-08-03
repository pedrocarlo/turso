//! Parser-expression conversion into resolved HIR expressions.

use turso_parser::ast;

use super::{
    analyze::Analyzer,
    context::DoubleQuotedDml,
    hir,
    scope::{NamePrecedence, OutputCollation, ResolvedScopeExpr, Scope},
};
use crate::{schema::Type, vdbe::affinity::Affinity, LimboError, Result};

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

impl Analyzer<'_, '_> {
    pub(crate) fn analyze_expr(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::Expr> {
        match syntax {
            ast::Expr::Literal(literal) => Ok(hir::Expr::Literal(literal.clone())),
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                if let Some(resolved) =
                    scope.resolve_unqualified(name.as_str(), policy.precedence)?
                {
                    return Ok(resolved.expr);
                }
                if policy.allow_dqs_fallback && name.quoted_with('"') {
                    return Ok(hir::Expr::Literal(ast::Literal::String(name.as_literal())));
                }
                crate::bail_parse_error!("no such column: {}", name.as_str());
            }
            ast::Expr::Qualified(table, column) => {
                if let Some(resolved) = scope.resolve_qualified(table.as_str(), column.as_str())? {
                    return Ok(resolved.expr);
                }
                crate::bail_parse_error!("no such table: {}", table.as_str());
            }
            ast::Expr::DoublyQualified(database, table, column) => {
                if let Some(database_id) = self.context().database(database.as_str()) {
                    if let Some(resolved) = scope.resolve_database_qualified(
                        database_id,
                        table.as_str(),
                        column.as_str(),
                    )? {
                        return Ok(resolved.expr);
                    }
                }
                crate::bail_parse_error!(
                    "no such column: {}.{}.{}",
                    database.as_str(),
                    table.as_str(),
                    column.as_str()
                );
            }
            _ => super::analyze::unsupported_select(),
        }
    }

    pub(super) fn resolve_expr_facts(
        &self,
        expression: hir::Expr,
        scope: &Scope,
    ) -> Result<ResolvedScopeExpr> {
        let (type_fact, affinity, has_affinity, collation) = match &expression {
            hir::Expr::Literal(literal) => (
                super::analyze::literal_type_fact(literal)?,
                Affinity::Blob,
                false,
                None,
            ),
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
                (
                    column.type_fact.clone(),
                    column.affinity,
                    column.has_affinity,
                    column.collation.clone(),
                )
            }
            hir::Expr::RowId(_) => (
                hir::TypeFact::known(Type::Integer),
                Affinity::Integer,
                true,
                None,
            ),
            hir::Expr::Output(output) => {
                let type_fact = scope.output_type(*output).cloned().ok_or_else(|| {
                    LimboError::InternalError(format!("missing output facts for {output:?}"))
                })?;
                let affinity = scope.output_affinity(*output).ok_or_else(|| {
                    LimboError::InternalError(format!("missing output affinity for {output:?}"))
                })?;
                let has_affinity = scope.output_has_affinity(*output).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing output affinity state for {output:?}"
                    ))
                })?;
                let collation = match scope.output_collation(*output) {
                    Some(OutputCollation::Absent) => None,
                    Some(OutputCollation::Inherited(collation)) => Some(collation.clone()),
                    Some(OutputCollation::Explicit(_)) => {
                        return super::analyze::unsupported_select();
                    }
                    None => {
                        return Err(LimboError::InternalError(format!(
                            "missing output collation state for {output:?}"
                        )))
                    }
                };
                (type_fact, affinity, has_affinity, collation)
            }
            _ => return super::analyze::unsupported_select(),
        };
        Ok(ResolvedScopeExpr {
            expr: expression,
            type_fact,
            affinity,
            has_affinity,
            collation,
        })
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
        Analyzer::new(&context).analyze_expr(syntax, scope, policy)
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
}
