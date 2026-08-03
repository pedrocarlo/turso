//! Semantic analysis from parser AST into resolved HIR.

mod analyze;
pub(crate) mod context;
pub(crate) mod hir;

pub(crate) use analyze::analyze;

pub(crate) enum AnalyzeInput<'ast> {
    Statement(&'ast turso_parser::ast::Stmt),
}
