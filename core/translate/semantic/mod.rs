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
    TriggerSelect {
        context: TriggerAnalysis,
        select: &'ast turso_parser::ast::Select,
    },
    TriggerInsert {
        context: TriggerAnalysis,
        insert: TriggerInsert<'ast>,
    },
    TriggerUpdate {
        context: TriggerAnalysis,
        update: TriggerUpdate<'ast>,
    },
}

pub(crate) struct TriggerInsert<'ast> {
    pub(crate) command_conflict: Option<turso_parser::ast::ResolveType>,
    pub(crate) conflict_override: Option<turso_parser::ast::ResolveType>,
    pub(crate) table: &'ast turso_parser::ast::Name,
    pub(crate) columns: &'ast [turso_parser::ast::Name],
    pub(crate) select: &'ast turso_parser::ast::Select,
    pub(crate) upsert: Option<&'ast turso_parser::ast::Upsert>,
    pub(crate) returning: &'ast [turso_parser::ast::ResultColumn],
}

pub(crate) struct TriggerUpdate<'ast> {
    pub(crate) command_conflict: Option<turso_parser::ast::ResolveType>,
    pub(crate) conflict_override: Option<turso_parser::ast::ResolveType>,
    pub(crate) table: &'ast turso_parser::ast::Name,
    pub(crate) assignments: &'ast [turso_parser::ast::Set],
    pub(crate) from: Option<&'ast turso_parser::ast::FromClause>,
    pub(crate) predicate: Option<&'ast turso_parser::ast::Expr>,
}

pub(crate) struct TriggerAnalysis {
    pub(crate) database: hir::DatabaseId,
    pub(crate) table: crate::sync::Arc<crate::schema::Table>,
    pub(crate) event: turso_parser::ast::TriggerEvent,
}
