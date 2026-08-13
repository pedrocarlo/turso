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

pub(crate) fn analyze_statement(
    catalog: &crate::translate::emitter::SemanticCatalogSnapshot,
    symbols: &crate::SymbolTable,
    dialect: crate::sync::Arc<dyn crate::dialect::Dialect>,
    custom_types_enabled: bool,
    dqs_dml: crate::translate::emitter::DoubleQuotedDml,
    statement: &turso_parser::ast::Stmt,
) -> crate::Result<hir::HirDocument> {
    let dqs_dml = match dqs_dml {
        crate::translate::emitter::DoubleQuotedDml::Enabled => context::DoubleQuotedDml::Enabled,
        crate::translate::emitter::DoubleQuotedDml::Disabled => context::DoubleQuotedDml::Disabled,
    };
    let context = context::SemanticContext::for_catalog_snapshot(
        catalog,
        symbols,
        custom_types_enabled,
        dialect,
        dqs_dml,
    )?;
    analyze::analyze(&context, AnalyzeInput::Statement(statement))
}

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
    TriggerDelete {
        context: TriggerAnalysis,
        delete: TriggerDelete<'ast>,
    },
    TriggerProgram(TriggerProgramInput<'ast>),
}

pub(crate) struct TriggerProgramInput<'ast> {
    pub(crate) context: TriggerAnalysis,
    pub(crate) predicate: Option<&'ast turso_parser::ast::Expr>,
    pub(crate) commands: &'ast [turso_parser::ast::TriggerCmd],
    pub(crate) conflict_override: Option<turso_parser::ast::ResolveType>,
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

pub(crate) struct TriggerDelete<'ast> {
    pub(crate) table: &'ast turso_parser::ast::Name,
    pub(crate) predicate: Option<&'ast turso_parser::ast::Expr>,
}

pub(crate) struct TriggerAnalysis {
    pub(crate) database: hir::DatabaseId,
    pub(crate) table: crate::sync::Arc<crate::schema::Table>,
    pub(crate) event: turso_parser::ast::TriggerEvent,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dialect::SqliteDialect,
        schema::{BTreeTable, Schema},
        sync::Arc,
        translate::emitter::{DoubleQuotedDml, Resolver},
        DatabaseCatalog, RwLock, SymbolTable, MAIN_DB_ID, TEMP_DB_ID,
    };
    use rustc_hash::FxHashMap as HashMap;
    use turso_parser::parser::Parser;

    #[test]
    fn owned_resolver_snapshot_produces_validated_hir() {
        let snapshot = {
            let mut main_schema = Schema::new();
            main_schema
                .add_btree_table(Arc::new(
                    BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                        .expect("table SQL parses"),
                ))
                .expect("table name is unique");
            let main_schema = Arc::new(main_schema);
            let database_schemas = RwLock::new(HashMap::default());
            let temp_database = RwLock::new(None);
            let attached_databases = RwLock::new(DatabaseCatalog::new());
            let symbols = SymbolTable::new();
            let resolver = Resolver::new(
                &main_schema,
                &database_schemas,
                &temp_database,
                &attached_databases,
                &symbols,
                true,
                DoubleQuotedDml::Disabled,
                Arc::new(SqliteDialect),
                &None,
            );
            resolver
                .semantic_catalog_snapshot(main_schema)
                .expect("resolver catalog freezes")
        };
        let symbols = SymbolTable::new();
        let ast::Cmd::Stmt(statement) = Parser::new(b"SELECT value FROM items")
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains statement")
        else {
            panic!("SQL contains ordinary statement");
        };

        let document = analyze_statement(
            &snapshot,
            &symbols,
            Arc::new(SqliteDialect),
            true,
            DoubleQuotedDml::Disabled,
            &statement,
        )
        .expect("owned resolver snapshot produces HIR");
        assert_eq!(
            document
                .databases
                .iter()
                .map(|database| database.database.index())
                .collect::<Vec<_>>(),
            [MAIN_DB_ID, TEMP_DB_ID]
        );
        document.validate().expect("document validates again");
    }
}
