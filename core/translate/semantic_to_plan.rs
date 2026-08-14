//! Adapts resolved semantic sources to existing planner structures.

use turso_parser::ast::{self, TableInternalId};

use super::{
    plan::{ColumnUsedMask, JoinInfo, JoinedTable, Operation},
    semantic::hir::{self, ColumnUsage, HirDocument, SourceId},
};
use crate::{
    vdbe::builder::{ProgramBuilder, TableRefIdCounter},
    LimboError, Result,
};

/// State shared while one HIR document is converted into existing plan nodes.
///
/// Planner table IDs remain necessary until plan and emitter column references
/// use [`SourceId`] directly. Allocating every source up front keeps captures
/// and nested queries on one stable mapping without changing planner logic.
pub(crate) struct HirPlanContext<'a> {
    document: &'a HirDocument,
    table_ids: Vec<TableInternalId>,
}

impl<'a> HirPlanContext<'a> {
    pub(crate) fn new(document: &'a HirDocument, program: &mut ProgramBuilder) -> Self {
        let table_ids =
            allocate_table_ids(document.sources.len(), &mut program.table_reference_counter);
        Self {
            document,
            table_ids,
        }
    }

    pub(crate) fn source(&self, id: SourceId) -> &'a hir::Source {
        self.document
            .source(id)
            .expect("validated HIR contains referenced source")
    }

    pub(crate) fn table_id(&self, source: SourceId) -> TableInternalId {
        self.table_ids[source.index()]
    }

    /// Build the existing planner representation without resolving names or
    /// reading the catalog again.
    pub(crate) fn joined_table(
        &self,
        source: SourceId,
        join_info: Option<JoinInfo>,
        usage: &[ColumnUsage],
    ) -> Result<JoinedTable> {
        joined_table_from_source(self.source(source), self.table_id(source), join_info, usage)
    }
}

fn joined_table_from_source(
    source: &hir::Source,
    internal_id: TableInternalId,
    join_info: Option<JoinInfo>,
    usage: &[ColumnUsage],
) -> Result<JoinedTable> {
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(format!(
            "source {} is not a table source",
            source.id
        )));
    };
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!("table source {} has no database", source.id))
    })?;
    let (col_used_mask, column_use_counts) = column_usage(source.id, usage)?;

    Ok(JoinedTable {
        op: Operation::default_scan_for(table.value()),
        table: table.value().clone(),
        identifier: source.alias.as_ref().unwrap_or(&source.name).clone(),
        internal_id,
        join_info,
        col_used_mask,
        column_use_counts,
        expression_index_usages: Vec::new(),
        database_id: database.index(),
        indexed: planner_index_hint(&source.index_hint),
    })
}

fn column_usage(source: SourceId, usage: &[ColumnUsage]) -> Result<(ColumnUsedMask, Vec<usize>)> {
    let mut mask = ColumnUsedMask::default();
    let mut counts = Vec::new();
    for usage in usage
        .iter()
        .filter(|usage| usage.reference.source == source)
    {
        mask.set(usage.reference.column)?;
        if counts.len() <= usage.reference.column {
            counts.resize(usage.reference.column + 1, 0);
        }
        counts[usage.reference.column] = usage.count;
    }
    Ok((mask, counts))
}

fn planner_index_hint(index_hint: &hir::IndexHint) -> Option<ast::Indexed> {
    match index_hint {
        hir::IndexHint::None => None,
        hir::IndexHint::NotIndexed => Some(ast::Indexed::NotIndexed),
        hir::IndexHint::Indexed(index) => Some(ast::Indexed::IndexedBy(ast::Name::exact(
            index.value().name.clone(),
        ))),
    }
}

fn allocate_table_ids(
    source_count: usize,
    counter: &mut TableRefIdCounter,
) -> Vec<TableInternalId> {
    (0..source_count).map(|_| counter.next()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        schema::{BTreeCharacteristics, BTreeTable, ColDef, Column, Index, Table, Type},
        sync::Arc,
        translate::semantic::hir::{
            CatalogObject, CatalogObjectId, CatalogSnapshot, ColumnReadExpression, DatabaseId,
            IndexCoverage, SourceColumn, SourceKind, SourceOwner, TypeFact,
        },
        vdbe::affinity::Affinity,
    };

    fn resolved_table(name: &str) -> hir::ResolvedTable {
        let columns = vec![
            Column::new(
                Some("first".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef::default(),
            ),
            Column::new(
                Some("second".to_string()),
                "TEXT".to_string(),
                None,
                None,
                Type::Text,
                None,
                ColDef::default(),
            ),
        ];
        let table = Table::BTree(Arc::new(BTreeTable::new(
            2,
            name.to_string(),
            vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        )));
        CatalogObject::new(
            CatalogObjectId::new(10),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(table),
        )
    }

    fn source() -> hir::Source {
        hir::Source {
            id: SourceId::new(0),
            owner: SourceOwner::Root,
            database: Some(DatabaseId::new(0)),
            name: "items".to_string(),
            alias: Some("i".to_string()),
            kind: SourceKind::Table(resolved_table("items")),
            columns: vec![
                SourceColumn {
                    name: "first".to_string(),
                    type_fact: TypeFact::known(Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                    hidden: false,
                    rowid_alias: false,
                },
                SourceColumn {
                    name: "second".to_string(),
                    type_fact: TypeFact::known(Type::Text),
                    affinity: Affinity::Text,
                    has_affinity: true,
                    collation: None,
                    hidden: false,
                    rowid_alias: false,
                },
            ],
            generated_expressions: vec![ColumnReadExpression::Absent; 2],
            default_expressions: vec![ColumnReadExpression::Absent; 2],
            column_type_programs: vec![None; 2],
            check_constraints: None,
            rowid_available: true,
            index_hint: hir::IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    #[test]
    fn source_ids_receive_stable_unique_planner_ids() {
        let mut counter = TableRefIdCounter::new();
        let table_ids = allocate_table_ids(3, &mut counter);

        assert_eq!(table_ids[0], TableInternalId::from(1));
        assert_eq!(table_ids[1], TableInternalId::from(2));
        assert_eq!(table_ids[2], TableInternalId::from(3));
    }

    #[test]
    fn planner_ids_allocated_after_sources_do_not_collide() {
        let mut counter = TableRefIdCounter::new();
        let table_ids = allocate_table_ids(3, &mut counter);
        let synthetic_id = counter.next();

        assert!(!table_ids.contains(&synthetic_id));
        assert_eq!(synthetic_id, TableInternalId::from(4));
    }

    #[test]
    fn table_source_reuses_resolved_metadata_and_alias() {
        let source = source();
        let SourceKind::Table(resolved) = &source.kind else {
            unreachable!();
        };
        let joined = joined_table_from_source(&source, TableInternalId::from(7), None, &[])
            .expect("table source converts");

        assert_eq!(joined.identifier, "i");
        assert_eq!(joined.internal_id, TableInternalId::from(7));
        assert_eq!(joined.database_id, 0);
        let (Table::BTree(expected), Table::BTree(actual)) = (resolved.value(), &joined.table)
        else {
            panic!("resolved and planned tables are btree tables");
        };
        assert!(Arc::ptr_eq(expected, actual));

        let mut unaliased = source;
        unaliased.alias = None;
        let joined = joined_table_from_source(&unaliased, TableInternalId::from(8), None, &[])
            .expect("unaliased table source converts");
        assert_eq!(joined.identifier, "items");
    }

    #[test]
    fn table_source_uses_hir_column_counts() {
        let source = source();
        let usage = [ColumnUsage {
            reference: hir::ColumnRef {
                source: source.id,
                column: 1,
            },
            count: 3,
        }];
        let joined = joined_table_from_source(&source, TableInternalId::from(7), None, &usage)
            .expect("table source converts");

        assert_eq!(
            (&joined.col_used_mask).into_iter().collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(joined.column_use_counts, vec![0, 3]);
    }

    #[test]
    fn table_source_preserves_index_hints_without_lookup() {
        let mut source = source();
        source.index_hint = hir::IndexHint::NotIndexed;
        let joined = joined_table_from_source(&source, TableInternalId::from(7), None, &[])
            .expect("table source converts");
        assert_eq!(joined.indexed, Some(ast::Indexed::NotIndexed));

        let index = Index {
            name: "items_second".to_string(),
            table_name: "items".to_string(),
            root_page: 3,
            columns: Vec::new(),
            unique: false,
            ephemeral: false,
            has_rowid: true,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        };
        source.index_hint = hir::IndexHint::Indexed(CatalogObject::new(
            CatalogObjectId::new(11),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(index),
        ));
        let joined = joined_table_from_source(&source, TableInternalId::from(7), None, &[])
            .expect("table source converts");
        assert!(matches!(
            joined.indexed,
            Some(ast::Indexed::IndexedBy(name)) if name.as_str() == "items_second"
        ));
    }

    #[test]
    fn non_table_source_is_rejected() {
        let mut source = source();
        source.kind = SourceKind::SchemaExpression;
        let error = joined_table_from_source(&source, TableInternalId::from(7), None, &[])
            .expect_err("schema namespace has no planner table");
        assert_eq!(
            error.to_string(),
            "Internal error: source s0 is not a table source"
        );
    }
}
