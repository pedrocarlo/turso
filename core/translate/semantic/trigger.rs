//! Trigger predicate conversion into resolved HIR.

use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    expr::ExprPolicy,
    hir::{self, CatalogObject, SourceOwner},
    query::table_has_rowid,
    scope::Scope,
    TriggerAnalysis,
};
use crate::{util::normalize_ident, LimboError, Result};

#[derive(Clone, Copy)]
enum TriggerRow {
    New,
    Old,
}

impl TriggerRow {
    fn name(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Old => "old",
        }
    }

    fn pseudo_source(self) -> hir::PseudoSource {
        match self {
            Self::New => hir::PseudoSource::New,
            Self::Old => hir::PseudoSource::Old,
        }
    }
}

impl<'ast> Analyzer<'_, '_, 'ast> {
    pub(super) fn analyze_trigger_predicate(
        &mut self,
        context: TriggerAnalysis,
        expression: &'ast ast::Expr,
    ) -> Result<hir::HirRoot> {
        let table_name = normalize_ident(context.table.get_name());
        let table_id =
            self.catalog_object_id(Some(context.database), CatalogObjectKind::Table, table_name);
        let table = CatalogObject::new(
            table_id,
            self.context().snapshot(),
            Some(context.database),
            context.table,
        );
        let analyzed_columns = self.source_columns(&table)?;
        let columns = analyzed_columns
            .iter()
            .map(|column| column.column.clone())
            .collect::<Vec<_>>();
        let column_type_programs = analyzed_columns
            .into_iter()
            .map(|column| column.programs)
            .collect::<Vec<_>>();
        let rowid_available = table_has_rowid(table.value());

        let mut create_row = |row| {
            self.create_trigger_row_source(
                row,
                &table,
                &columns,
                &column_type_programs,
                rowid_available,
            )
        };
        let (new_source, old_source) = match &context.event {
            ast::TriggerEvent::Insert => (Some(create_row(TriggerRow::New)?), None),
            ast::TriggerEvent::Update | ast::TriggerEvent::UpdateOf(_) => (
                Some(create_row(TriggerRow::New)?),
                Some(create_row(TriggerRow::Old)?),
            ),
            ast::TriggerEvent::Delete => (None, Some(create_row(TriggerRow::Old)?)),
        };
        let environment = hir::TriggerEnvironment {
            table,
            new_source,
            old_source,
        };

        let mut scope = Scope::default();
        for source in [environment.new_source, environment.old_source]
            .into_iter()
            .flatten()
        {
            scope.add_source(
                self.source(source).ok_or_else(|| {
                    LimboError::InternalError(format!("missing trigger row source {source}"))
                })?,
                false,
            );
        }
        let expression = self
            .analyze_root_expr(
                expression,
                &scope,
                ExprPolicy::trigger_predicate(self.context().dqs_dml(), &context.event),
            )?
            .expr;

        Ok(hir::HirRoot::TriggerPredicate(hir::TriggerPredicate {
            expression,
            environment,
        }))
    }

    fn create_trigger_row_source(
        &mut self,
        row: TriggerRow,
        table: &hir::ResolvedTable,
        columns: &[hir::SourceColumn],
        column_type_programs: &[Option<hir::BoundColumnTypePrograms>],
        rowid_available: bool,
    ) -> Result<hir::SourceId> {
        let source = self.reserve_source();
        let name = row.name();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner: SourceOwner::Root,
                database: table.database(),
                name: name.to_string(),
                alias: None,
                kind: hir::SourceKind::Pseudo {
                    kind: row.pseudo_source(),
                    table: table.clone(),
                },
                columns: columns.to_vec(),
                generated_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                default_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                column_type_programs: column_type_programs.to_vec(),
                check_constraints: None,
                rowid_available,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
    }
}
