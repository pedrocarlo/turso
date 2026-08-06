//! Query-source conversion into HIR arenas and scopes.

use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    expr::{build_using_column, ExprPolicy},
    hir::{self, CatalogObject, DeclaredType, SourceOwner, TypeFact},
    scope::{resolve_source_column, Scope},
};
use crate::{schema::Table, sync::Arc, Result};

impl Analyzer<'_, '_, '_> {
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
            let kind = join_kind(syntax_join.operator);
            if kind == hir::JoinKind::Right && !joins.is_empty() {
                crate::bail_parse_error!(
                    "RIGHT JOIN following another join is not yet supported. \
                     Try rewriting as LEFT JOIN or using a subquery."
                );
            }
            let natural = is_natural_join(syntax_join.operator);
            if natural && syntax_join.constraint.is_some() {
                crate::bail_parse_error!("a NATURAL join may not have an ON or USING clause");
            }
            let right = self.analyze_table_source(&syntax_join.table, owner)?;
            let definition = self.source(right).ok_or_else(|| {
                crate::LimboError::InternalError(format!("missing semantic source {right}"))
            })?;

            let using_names = if natural {
                Some(scope.natural_common_columns(definition))
            } else if let Some(ast::JoinConstraint::Using(names)) = &syntax_join.constraint {
                Some(names.iter().map(|name| name.as_str().to_string()).collect())
            } else {
                None
            };
            let using_columns = using_names
                .map(|names| {
                    names
                        .into_iter()
                        .map(|name| {
                            let left = if natural {
                                scope.resolve_natural_left(&name)?
                            } else {
                                scope.resolve_using_left(&name)?
                            };
                            let right = resolve_source_column(definition, &name)?;
                            build_using_column(name, left, right, merged_column_value(kind))
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;

            scope.add_source(definition, true);
            if let Some(columns) = &using_columns {
                scope.apply_using(columns)?;
            }
            let constraint = match using_columns {
                Some(columns) if natural => hir::JoinConstraint::Natural(columns),
                Some(columns) => hir::JoinConstraint::Using(columns),
                None => hir::JoinConstraint::None,
            };
            joins.push(hir::Join {
                right,
                kind,
                constraint,
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
        if name.db_name.is_none() {
            if let Some(cte) = self.resolve_cte(name.name.as_str())? {
                return self.analyze_cte_source(
                    cte,
                    name.name.as_str(),
                    alias
                        .as_ref()
                        .map(ast::As::name)
                        .or(name.alias.as_ref())
                        .map(ast::Name::as_str),
                    owner,
                );
            }
        }
        self.analyze_base_table_source(name, alias.as_ref(), indexed.as_ref(), owner)
    }

    fn analyze_cte_source(
        &mut self,
        cte: hir::CteId,
        name: &str,
        alias: Option<&str>,
        owner: SourceOwner,
    ) -> Result<hir::SourceId> {
        let cte_definition = self.cte(cte).ok_or_else(|| {
            crate::LimboError::InternalError(format!("missing semantic CTE {cte}"))
        })?;
        let columns = cte_definition
            .columns
            .iter()
            .map(|column| hir::SourceColumn {
                name: column.name.clone(),
                type_fact: column.type_fact.clone(),
                affinity: column.affinity,
                has_affinity: column.has_affinity,
                collation: column.collation.clone(),
                hidden: false,
                rowid_alias: false,
            })
            .collect::<Vec<_>>();
        let width = columns.len();
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner,
                database: None,
                name: crate::util::normalize_ident(name),
                alias: alias.map(crate::util::normalize_ident),
                kind: hir::SourceKind::Cte(cte),
                columns,
                generated_expressions: vec![hir::ColumnReadExpression::Absent; width],
                default_expressions: vec![hir::ColumnReadExpression::Absent; width],
                column_type_programs: vec![None; width],
                check_constraints: None,
                rowid_available: false,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
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

fn join_kind(operator: ast::JoinOperator) -> hir::JoinKind {
    match operator {
        ast::JoinOperator::Comma => hir::JoinKind::Comma,
        ast::JoinOperator::TypedJoin(None) => hir::JoinKind::Inner,
        ast::JoinOperator::TypedJoin(Some(kind)) if is_full_join(kind) => hir::JoinKind::Full,
        ast::JoinOperator::TypedJoin(Some(kind)) if kind.contains(ast::JoinType::RIGHT) => {
            hir::JoinKind::Right
        }
        ast::JoinOperator::TypedJoin(Some(kind)) if kind.contains(ast::JoinType::LEFT) => {
            hir::JoinKind::Left
        }
        ast::JoinOperator::TypedJoin(Some(kind)) if kind.contains(ast::JoinType::CROSS) => {
            hir::JoinKind::Cross
        }
        ast::JoinOperator::TypedJoin(Some(_)) => hir::JoinKind::Inner,
    }
}

fn is_full_join(kind: ast::JoinType) -> bool {
    let left = kind.contains(ast::JoinType::LEFT);
    let right = kind.contains(ast::JoinType::RIGHT);
    let outer = kind.contains(ast::JoinType::OUTER);
    (left && right) || (outer && !left && !right)
}

fn merged_column_value(kind: hir::JoinKind) -> hir::MergedColumnValue {
    match kind {
        hir::JoinKind::Right => hir::MergedColumnValue::Right,
        hir::JoinKind::Full => hir::MergedColumnValue::Coalesce,
        hir::JoinKind::Comma
        | hir::JoinKind::Inner
        | hir::JoinKind::Cross
        | hir::JoinKind::Left => hir::MergedColumnValue::Left,
    }
}

fn is_natural_join(operator: ast::JoinOperator) -> bool {
    matches!(
        operator,
        ast::JoinOperator::TypedJoin(Some(kind)) if kind.contains(ast::JoinType::NATURAL)
    )
}

fn table_has_rowid(table: &Table) -> bool {
    table.btree().is_some_and(|table| table.has_rowid) || table.virtual_table().is_some()
}
