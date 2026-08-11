use std::sync::atomic::{AtomicU64, Ordering};

use rustc_hash::FxHashMap as HashMap;

use crate::{
    dialect::Dialect,
    function::Func,
    schema::{Index, Schema, Table},
    sync::Arc,
    util::normalize_ident,
    LimboError, Result, SymbolTable, MAIN_DB_ID,
};

use super::hir::{CatalogSnapshot, DatabaseId};

static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

/// Whether unresolved double-quoted DML names become string literals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DoubleQuotedDml {
    Enabled,
    Disabled,
}

impl DoubleQuotedDml {
    pub(crate) const fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Catalog inputs available while converting parser AST into resolved HIR.
pub(crate) struct SemanticContext<'catalog> {
    main_schema: &'catalog Schema,
    symbols: &'catalog SymbolTable,
    custom_types_enabled: bool,
    dialect: Arc<dyn Dialect>,
    database_names: HashMap<String, DatabaseId>,
    dqs_dml: DoubleQuotedDml,
    snapshot: CatalogSnapshot,
}

impl<'catalog> SemanticContext<'catalog> {
    pub(crate) fn for_main_schema_object(
        main_schema: &'catalog Schema,
        symbols: &'catalog SymbolTable,
        custom_types_enabled: bool,
        dialect: Arc<dyn Dialect>,
    ) -> Self {
        let snapshot = NEXT_SNAPSHOT_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("catalog snapshot identity space exhausted");
        let mut database_names = HashMap::default();
        database_names.insert("main".to_string(), DatabaseId::new(MAIN_DB_ID));
        Self {
            main_schema,
            symbols,
            custom_types_enabled,
            dialect,
            database_names,
            dqs_dml: DoubleQuotedDml::Enabled,
            snapshot: CatalogSnapshot::from_id(snapshot),
        }
    }

    pub(crate) fn with_dqs_dml(mut self, dqs_dml: DoubleQuotedDml) -> Self {
        self.dqs_dml = dqs_dml;
        self
    }

    pub(crate) const fn main_schema(&self) -> &Schema {
        self.main_schema
    }

    pub(crate) const fn symbols(&self) -> &SymbolTable {
        self.symbols
    }

    pub(crate) const fn custom_types_enabled(&self) -> bool {
        self.custom_types_enabled
    }

    pub(crate) fn dialect(&self) -> &Arc<dyn Dialect> {
        &self.dialect
    }

    pub(crate) fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        match self.dialect.resolve_function(name, arg_count)? {
            Some(function) => Ok(Some(function)),
            None => Ok(self
                .symbols
                .resolve_function(name, arg_count)
                .map(Func::External)),
        }
    }

    pub(crate) fn database(&self, name: &str) -> Option<DatabaseId> {
        self.database_names.get(&normalize_ident(name)).copied()
    }

    pub(crate) fn resolve_table(
        &self,
        name: &turso_parser::ast::QualifiedName,
    ) -> Result<(DatabaseId, Arc<Table>)> {
        let database = match &name.db_name {
            Some(database) => self.database(database.as_str()).ok_or_else(|| {
                LimboError::InvalidArgument(format!(
                    "no such database: {}",
                    normalize_ident(database.as_str())
                ))
            })?,
            None => DatabaseId::new(MAIN_DB_ID),
        };
        let table_name = normalize_ident(name.name.as_str());
        let table = self
            .main_schema
            .get_table(&table_name)
            .ok_or_else(|| LimboError::ParseError(format!("no such table: {table_name}")))?;
        Ok((database, table))
    }

    pub(crate) fn resolve_index(&self, table_name: &str, index_name: &str) -> Result<Arc<Index>> {
        let normalized_name = normalize_ident(index_name);
        self.main_schema
            .get_index(table_name, &normalized_name)
            .cloned()
            .ok_or_else(|| LimboError::ParseError(format!("no such index: {index_name}")))
    }

    pub(crate) const fn dqs_dml(&self) -> DoubleQuotedDml {
        self.dqs_dml
    }

    pub(crate) const fn snapshot(&self) -> CatalogSnapshot {
        self.snapshot
    }
}
