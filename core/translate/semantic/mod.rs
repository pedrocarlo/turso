//! Semantic analysis from parser AST into resolved HIR.

mod analyze;
pub(crate) mod context;
mod cte;
mod delete;
mod dml;
mod expr;
pub(crate) mod hir;
mod insert;
mod query;
mod schema_program;
mod scope;
mod trigger;
mod update;

pub(crate) use analyze::analyze;

pub(crate) enum AnalyzeInput<'ast> {
    Statement(&'ast turso_parser::ast::Stmt),
    TriggerPredicate {
        context: TriggerAnalysis,
        expression: &'ast turso_parser::ast::Expr,
    },
}

pub(crate) struct TriggerAnalysis {
    pub(crate) database: hir::DatabaseId,
    pub(crate) table: crate::sync::Arc<crate::schema::Table>,
    pub(crate) event: turso_parser::ast::TriggerEvent,
}
