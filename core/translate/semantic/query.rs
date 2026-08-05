//! Query-source conversion into HIR arenas and scopes.

use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    expr::ExprPolicy,
    hir::{self, CatalogObject, DeclaredType, SourceOwner, TypeFact},
    scope::Scope,
};
use crate::{schema::Table, sync::Arc, Result};

impl Analyzer<'_, '_> {
    pub(super) fn analyze_from_clause(
        &mut self,
        syntax: &ast::FromClause,
        owner: SourceOwner,
    ) -> Result<(hir::From, Scope)> {
        let source = self.analyze_table_source(&syntax.select, owner)?;
        let mut scope = Scope::default();
        let definition = self.source(source).ok_or_else(|| {
            crate::LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        scope.add_source(definition, true);

        let mut joins = Vec::with_capacity(syntax.joins.len());
        for syntax_join in &syntax.joins {
            let kind = basic_join_kind(syntax_join.operator)?;
            if matches!(syntax_join.constraint, Some(ast::JoinConstraint::Using(_))) {
                return super::analyze::unsupported_select();
            }
            let right = self.analyze_table_source(&syntax_join.table, owner)?;
            let definition = self.source(right).ok_or_else(|| {
                crate::LimboError::InternalError(format!("missing semantic source {right}"))
            })?;
            scope.add_source(definition, true);
            joins.push(hir::Join {
                right,
                kind,
                constraint: hir::JoinConstraint::None,
            });
        }

        let policy = ExprPolicy::select(self.context().dqs_dml());
        for (syntax_join, join) in syntax.joins.iter().zip(&mut joins) {
            if let Some(ast::JoinConstraint::On(expression)) = &syntax_join.constraint {
                join.constraint =
                    hir::JoinConstraint::On(self.analyze_expr(expression, &scope, policy)?);
            }
        }

        Ok((
            hir::From {
                first: source,
                joins,
            },
            scope,
        ))
    }

    fn analyze_table_source(
        &mut self,
        syntax: &ast::SelectTable,
        owner: SourceOwner,
    ) -> Result<hir::SourceId> {
        let ast::SelectTable::Table(name, alias, indexed) = syntax else {
            return super::analyze::unsupported_select();
        };
        self.analyze_base_table_source(name, alias.as_ref(), indexed.as_ref(), owner)
    }

    fn analyze_base_table_source(
        &mut self,
        name: &ast::QualifiedName,
        alias: Option<&ast::As>,
        indexed: Option<&ast::Indexed>,
        owner: SourceOwner,
    ) -> Result<hir::SourceId> {
        let (database, table) = self.context().resolve_table(name)?;
        let table_name = crate::util::normalize_ident(name.name.as_str());
        let table_id =
            self.catalog_object_id(Some(database), CatalogObjectKind::Table, table_name.clone());
        let table = CatalogObject::new(table_id, self.context().snapshot(), Some(database), table);
        let columns = self.source_columns(&table)?;
        let generated_expressions = table
            .value()
            .columns()
            .iter()
            .map(|column| {
                if column.generated_expr().is_some() {
                    hir::ColumnReadExpression::NotRequired
                } else {
                    hir::ColumnReadExpression::Absent
                }
            })
            .collect();
        let default_expressions = table
            .value()
            .columns()
            .iter()
            .map(|column| {
                if column.default.is_some() {
                    hir::ColumnReadExpression::NotRequired
                } else {
                    hir::ColumnReadExpression::Absent
                }
            })
            .collect();
        let index_hint = match indexed {
            None => hir::IndexHint::None,
            Some(ast::Indexed::NotIndexed) => hir::IndexHint::NotIndexed,
            Some(ast::Indexed::IndexedBy(_)) => return super::analyze::unsupported_select(),
        };
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner,
                database: Some(database),
                name: table_name,
                alias: alias
                    .map(ast::As::name)
                    .or(name.alias.as_ref())
                    .map(|name| crate::util::normalize_ident(name.as_str())),
                kind: hir::SourceKind::Table(table.clone()),
                columns,
                generated_expressions,
                default_expressions,
                column_type_programs: vec![None; table.value().columns().len()],
                check_constraints: None,
                rowid_available: table_has_rowid(table.value()),
                index_hint,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
    }

    fn source_columns(&mut self, table: &hir::ResolvedTable) -> Result<Vec<hir::SourceColumn>> {
        let is_strict = matches!(table.value(), Table::BTree(table) if table.is_strict);
        let mut columns = Vec::with_capacity(table.value().columns().len());
        for (index, column) in table.value().columns().iter().enumerate() {
            if self
                .context()
                .main_schema()
                .get_type_def(&column.ty_str, is_strict)
                .is_some()
            {
                return super::analyze::unsupported_select();
            }
            let type_fact = if column.ty_str.is_empty() {
                TypeFact::known(column.ty())
            } else {
                TypeFact::declared(DeclaredType {
                    name: column.ty_str.clone(),
                    storage: column.ty(),
                    custom_chain: Vec::new(),
                    array_dimensions: column.array_dimensions(),
                })
            };
            let collation = column.collation_opt().map(|collation| {
                let name = collation.to_string();
                let id = self.catalog_object_id(None, CatalogObjectKind::Collation, name);
                CatalogObject::new(id, self.context().snapshot(), None, Arc::new(collation))
            });
            columns.push(hir::SourceColumn {
                name: column
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("column{}", index + 1)),
                type_fact,
                affinity: column.affinity_with_strict(is_strict),
                has_affinity: true,
                collation,
                hidden: column.hidden(),
                rowid_alias: column.is_rowid_alias(),
            });
        }
        Ok(columns)
    }
}

fn basic_join_kind(operator: ast::JoinOperator) -> Result<hir::JoinKind> {
    match operator {
        ast::JoinOperator::Comma => Ok(hir::JoinKind::Comma),
        ast::JoinOperator::TypedJoin(None) => Ok(hir::JoinKind::Inner),
        ast::JoinOperator::TypedJoin(Some(kind))
            if kind.intersects(
                ast::JoinType::NATURAL
                    | ast::JoinType::LEFT
                    | ast::JoinType::RIGHT
                    | ast::JoinType::OUTER,
            ) =>
        {
            super::analyze::unsupported_select()
        }
        ast::JoinOperator::TypedJoin(Some(kind)) if kind.contains(ast::JoinType::CROSS) => {
            Ok(hir::JoinKind::Cross)
        }
        ast::JoinOperator::TypedJoin(Some(_)) => Ok(hir::JoinKind::Inner),
    }
}

fn table_has_rowid(table: &Table) -> bool {
    table.btree().is_some_and(|table| table.has_rowid) || table.virtual_table().is_some()
}
