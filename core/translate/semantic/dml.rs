//! Shared semantic helpers for data-changing statements.

use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    hir,
};
use crate::{schema::Table, sync::Arc, util::normalize_ident, LimboError, Result};

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

    pub(super) fn analyze_dml_foreign_keys(
        &mut self,
        target: &hir::ResolvedTable,
        outgoing_child_source: hir::SourceId,
    ) -> Result<hir::DmlForeignKeys> {
        let database = target.database().ok_or_else(|| {
            LimboError::InternalError("DML target has no owning database".to_string())
        })?;
        let (outgoing, incoming) = {
            let schema = self.context().main_schema();
            (
                schema.resolved_fks_for_child(target.value().get_name())?,
                schema.resolved_fks_referencing(target.value().get_name())?,
            )
        };

        let mut resolved_outgoing = Vec::with_capacity(outgoing.len());
        for foreign_key in outgoing {
            let parent_table =
                self.freeze_foreign_key_table(database, &foreign_key.fk.parent_table, "parent")?;
            resolved_outgoing.push(self.freeze_foreign_key(
                database,
                outgoing_child_source,
                target.clone(),
                parent_table,
                foreign_key,
            ));
        }

        let mut resolved_incoming = Vec::with_capacity(incoming.len());
        for foreign_key in incoming {
            let child_name = foreign_key.child_table.name.clone();
            let child_source = self.analyze_base_table_source(
                &ast::QualifiedName {
                    db_name: None,
                    name: ast::Name::exact(child_name.clone()),
                    alias: None,
                },
                None,
                None,
                hir::SourceOwner::Root,
            )?;
            let child_table = match &self
                .source(child_source)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing foreign-key child source {child_source}"
                    ))
                })?
                .kind
            {
                hir::SourceKind::Table(table) => table.clone(),
                _ => {
                    return Err(LimboError::InternalError(format!(
                        "foreign-key child {child_name} is not a catalog table"
                    )));
                }
            };
            resolved_incoming.push(self.freeze_foreign_key(
                database,
                child_source,
                child_table,
                target.clone(),
                foreign_key,
            ));
        }

        Ok(hir::DmlForeignKeys {
            outgoing: resolved_outgoing,
            incoming: resolved_incoming,
        })
    }

    fn freeze_foreign_key_table(
        &mut self,
        database: hir::DatabaseId,
        name: &str,
        role: &str,
    ) -> Result<hir::ResolvedTable> {
        let normalized = normalize_ident(name);
        let table = self
            .context()
            .main_schema()
            .get_table(&normalized)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "resolved foreign-key {role} table {normalized} is missing"
                ))
            })?;
        let id = self.catalog_object_id(Some(database), CatalogObjectKind::Table, normalized);
        Ok(hir::CatalogObject::new(
            id,
            self.context().snapshot(),
            Some(database),
            table,
        ))
    }

    fn freeze_foreign_key(
        &mut self,
        database: hir::DatabaseId,
        child_source: hir::SourceId,
        child_table: hir::ResolvedTable,
        parent_table: hir::ResolvedTable,
        foreign_key: crate::schema::ResolvedFkRef,
    ) -> hir::ResolvedForeignKey {
        let parent_unique_index = foreign_key.parent_unique_index.map(|index| {
            let id = self.catalog_object_id(
                Some(database),
                CatalogObjectKind::Index,
                normalize_ident(&index.name),
            );
            hir::CatalogObject::new(id, self.context().snapshot(), Some(database), index)
        });
        hir::ResolvedForeignKey {
            child_table,
            child_source,
            parent_table,
            declaration: foreign_key.fk,
            parent_columns: foreign_key.parent_cols,
            child_positions: foreign_key.child_pos,
            parent_positions: foreign_key.parent_pos,
            parent_uses_rowid: foreign_key.parent_uses_rowid,
            parent_unique_index,
            parent_action_guarantees_new_parent: false,
        }
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
