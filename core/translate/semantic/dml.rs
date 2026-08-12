//! Shared semantic helpers for data-changing statements.

use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    hir,
};
use crate::{schema::Table, sync::Arc, util::normalize_ident};

impl Analyzer<'_, '_, '_> {
    pub(super) fn freeze_trigger(
        &mut self,
        database: hir::DatabaseId,
        trigger: Arc<crate::schema::Trigger>,
    ) -> hir::ResolvedTrigger {
        let id = self.catalog_object_id(
            Some(database),
            CatalogObjectKind::Trigger,
            normalize_ident(&trigger.name),
        );
        hir::CatalogObject::new(id, self.context().snapshot(), Some(database), trigger)
    }
}

pub(super) fn trigger_targets_database(
    trigger: &crate::schema::Trigger,
    database: hir::DatabaseId,
) -> bool {
    trigger
        .target_database_id
        .is_none_or(|target| target == database.index())
}

pub(super) fn trigger_matches_update(
    trigger: &crate::schema::Trigger,
    table: &Table,
    updated_columns: &[usize],
) -> bool {
    match &trigger.event {
        ast::TriggerEvent::Update => true,
        ast::TriggerEvent::UpdateOf(columns) => columns.iter().any(|column| {
            table
                .get_column_by_name(&normalize_ident(column.as_str()))
                .is_some_and(|(position, _)| updated_columns.contains(&position))
        }),
        ast::TriggerEvent::Delete | ast::TriggerEvent::Insert => false,
    }
}
