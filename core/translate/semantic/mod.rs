//! Semantic analysis from parser AST into resolved HIR.

mod analyze;
pub(crate) mod context;
mod expr;
pub(crate) mod hir;
mod query;
mod schema_program;
mod scope;

pub(crate) use analyze::analyze;

pub(crate) enum AnalyzeInput<'ast> {
    Statement(&'ast turso_parser::ast::Stmt),
}
