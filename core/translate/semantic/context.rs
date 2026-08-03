use std::sync::atomic::{AtomicU64, Ordering};

use crate::{dialect::Dialect, schema::Schema, sync::Arc, SymbolTable};

use super::hir::CatalogSnapshot;

static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

/// Catalog inputs available while converting parser AST into resolved HIR.
pub(crate) struct SemanticContext<'catalog> {
    main_schema: &'catalog Schema,
    symbols: &'catalog SymbolTable,
    custom_types_enabled: bool,
    dialect: Arc<dyn Dialect>,
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
        Self {
            main_schema,
            symbols,
            custom_types_enabled,
            dialect,
            snapshot: CatalogSnapshot::from_id(snapshot),
        }
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

    pub(crate) const fn snapshot(&self) -> CatalogSnapshot {
        self.snapshot
    }
}
