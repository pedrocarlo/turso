//! Adapts resolved semantic sources to existing planner identities.

use turso_parser::ast::TableInternalId;

use super::semantic::hir::{self, HirDocument, SourceId};
use crate::vdbe::builder::{ProgramBuilder, TableRefIdCounter};

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
}
