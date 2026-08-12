//! INSERT conversion into closed semantic HIR.

use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    expr::ExprPolicy,
    hir::{self, HirRoot, SourceOwner},
    scope::Scope,
};
use crate::{
    schema::Table,
    translate::expr::{walk_expr, WalkControl},
    util::normalize_ident,
    LimboError, Result,
};

impl<'ast> Analyzer<'_, '_, 'ast> {
    pub(super) fn analyze_insert(
        &mut self,
        with: Option<&'ast ast::With>,
        conflict: Option<ast::ResolveType>,
        table_name: &ast::QualifiedName,
        column_names: &[ast::Name],
        body: &'ast ast::InsertBody,
        returning: &[ast::ResultColumn],
    ) -> Result<HirRoot> {
        if conflict.is_some() {
            return unsupported_insert("conflict resolution");
        }
        if !returning.is_empty() {
            return unsupported_insert("RETURNING clauses");
        }
        if with.is_some()
            && !matches!(
                body,
                ast::InsertBody::Select(select, _)
                    if matches!(select.body.select, ast::OneSelect::Select { .. })
            )
        {
            return unsupported_insert("WITH clauses on non-query sources");
        }

        let target = self.analyze_base_table_source(table_name, None, None, SourceOwner::Root)?;
        let table = match &self
            .source(target)
            .ok_or_else(|| {
                LimboError::InternalError(format!("missing INSERT target source {target}"))
            })?
            .kind
        {
            hir::SourceKind::Table(table) => table.clone(),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "INSERT target {target} is not a catalog table"
                )));
            }
        };
        self.require_basic_insert_target(table.value())?;
        self.analyze_insert_target_metadata(target, &table)?;

        let (columns, source) = match body {
            ast::InsertBody::DefaultValues => {
                if !column_names.is_empty() {
                    return unsupported_insert("a column list with DEFAULT VALUES");
                }
                (Vec::new(), hir::InsertSource::DefaultValues)
            }
            ast::InsertBody::Select(select, upsert) => {
                if upsert.is_some() {
                    return unsupported_insert("UPSERT clauses");
                }
                let columns = resolve_insert_targets(table.value(), column_names)?;
                let expected = columns.len();
                match &select.body.select {
                    ast::OneSelect::Values(_) => {
                        let rows = simple_values_rows(select)?;
                        if rows.is_empty() {
                            crate::bail_parse_error!("no values to insert");
                        }
                        let mut bound_rows = Vec::with_capacity(rows.len());
                        for row in rows {
                            if row.len() != expected {
                                crate::bail_parse_error!(
                                    "table {} has {expected} columns but {} values were supplied",
                                    table.value().get_name(),
                                    row.len()
                                );
                            }
                            let mut bound = Vec::with_capacity(row.len());
                            for (syntax, target_column) in row.iter().zip(&columns) {
                                bound.push(self.analyze_insert_value(
                                    syntax,
                                    target,
                                    table.value(),
                                    target_column.column,
                                )?);
                            }
                            bound_rows.push(bound);
                        }
                        (columns, hir::InsertSource::Values(bound_rows))
                    }
                    ast::OneSelect::Select { .. } => {
                        let query = self.analyze_insert_select(select, with)?;
                        let actual = self
                            .query(query)
                            .ok_or_else(|| {
                                LimboError::InternalError(format!(
                                    "missing INSERT source query {query}"
                                ))
                            })?
                            .output
                            .len();
                        if actual != expected {
                            crate::bail_parse_error!(
                                "table {} has {expected} columns but {actual} values were supplied",
                                table.value().get_name()
                            );
                        }
                        (columns, hir::InsertSource::Query(query))
                    }
                }
            }
        };
        let defaults = self.analyze_insert_defaults(target, table.value(), &columns)?;

        Ok(HirRoot::Insert(hir::Insert {
            target,
            autoincrement: None,
            autoincrement_sequence: None,
            columns,
            defaults,
            source,
            conflict: None,
            upserts: Vec::new(),
            excluded_source: None,
            returning: None,
            trigger: None,
            triggers: Vec::new(),
            upsert_triggers: Vec::new(),
            foreign_keys: hir::DmlForeignKeys::default(),
        }))
    }

    fn analyze_insert_select(
        &mut self,
        select: &'ast ast::Select,
        with: Option<&'ast ast::With>,
    ) -> Result<hir::QueryId> {
        if let Some(with) = with {
            self.push_cte_scope(with)?;
        }
        let result = self.analyze_select(select);
        if with.is_some() {
            self.cte_scopes
                .pop()
                .expect("an INSERT WITH clause must own one CTE scope");
        }
        result
    }

    fn require_basic_insert_target(&self, table: &Table) -> Result<()> {
        let Some(table) = table.btree() else {
            return unsupported_insert("non-B-tree targets");
        };
        if table.has_autoincrement {
            return unsupported_insert("AUTOINCREMENT targets");
        }
        let schema = self.context().main_schema();
        if schema.get_triggers_for_table(&table.name).next().is_some() {
            return unsupported_insert("triggered targets");
        }
        if schema.has_child_fks(&table.name) || schema.any_resolved_fks_referencing(&table.name) {
            return unsupported_insert("foreign-key targets");
        }
        Ok(())
    }

    fn analyze_insert_target_metadata(
        &mut self,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
    ) -> Result<()> {
        let btree = table.value().btree().ok_or_else(|| {
            LimboError::InternalError(format!(
                "INSERT target {} stopped being a B-tree table",
                table.value().get_name()
            ))
        })?;
        let scope = {
            let source = self.source(target).ok_or_else(|| {
                LimboError::InternalError(format!("missing INSERT target source {target}"))
            })?;
            let mut scope = Scope::default();
            scope.add_source(source, true);
            scope
        };
        let policy = ExprPolicy::schema_expression().with_self_source(target);

        let mut check_constraints = Vec::with_capacity(btree.check_constraints.len());
        for (catalog_position, constraint) in btree.check_constraints.iter().enumerate() {
            check_constraints.push(hir::CheckConstraint {
                catalog_position,
                expression: self.analyze_expr(&constraint.expr, &scope, policy)?,
                description: constraint
                    .name
                    .clone()
                    .unwrap_or_else(|| constraint.expr.to_string()),
            });
        }

        let indexes = self
            .context()
            .main_schema()
            .get_indices(table.value().get_name())
            .cloned()
            .collect::<Vec<_>>();
        let mut index_expressions = Vec::with_capacity(indexes.len());
        let mut index_ids = Vec::with_capacity(indexes.len());
        for index in indexes {
            let index_id = self.catalog_object_id(
                table.database(),
                CatalogObjectKind::Index,
                normalize_ident(&index.name),
            );
            let resolved = hir::CatalogObject::new(
                index_id,
                self.context().snapshot(),
                table.database(),
                index,
            );
            let columns = resolved
                .value()
                .columns
                .iter()
                .map(|column| {
                    column
                        .expr
                        .as_deref()
                        .map(|syntax| self.analyze_expr(syntax, &scope, policy))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let predicate = resolved
                .value()
                .where_clause
                .as_deref()
                .map(|syntax| self.analyze_expr(syntax, &scope, policy))
                .transpose()?;
            index_ids.push(index_id);
            index_expressions.push(hir::IndexExpressions {
                index: resolved,
                columns,
                predicate,
            });
        }

        let source = self.source_mut(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing INSERT target source {target}"))
        })?;
        source.check_constraints = Some(check_constraints);
        source.index_expressions = index_expressions;
        source.index_coverage = hir::IndexCoverage::Complete { indexes: index_ids };
        Ok(())
    }

    fn analyze_insert_value(
        &mut self,
        syntax: &ast::Expr,
        target: hir::SourceId,
        table: &Table,
        column: hir::TargetColumn,
    ) -> Result<hir::Expr> {
        if matches!(syntax, ast::Expr::Default) {
            return self.analyze_default_value(target, table, column);
        }
        if expression_contains_subquery(syntax) {
            return unsupported_insert("subqueries in VALUES rows");
        }
        self.analyze_expr(
            syntax,
            &Scope::default(),
            ExprPolicy::insert_values(self.context().dqs_dml()),
        )
    }

    fn analyze_insert_defaults(
        &mut self,
        target: hir::SourceId,
        table: &Table,
        columns: &[hir::InsertTarget],
    ) -> Result<Vec<hir::ResolvedDefault>> {
        let mut supplied = vec![false; table.columns().len()];
        for target in columns {
            if let hir::TargetColumn::Column(column) = target.column {
                supplied[column] = true;
            }
        }
        let missing = table
            .columns()
            .iter()
            .enumerate()
            .filter(|(column, definition)| {
                !definition.hidden() && !definition.is_generated() && !supplied[*column]
            })
            .map(|(column, _)| column)
            .collect::<Vec<_>>();
        let mut defaults = Vec::with_capacity(missing.len());
        for column in missing {
            defaults.push(hir::ResolvedDefault {
                column,
                value: self.analyze_default_value(
                    target,
                    table,
                    hir::TargetColumn::Column(column),
                )?,
            });
        }
        Ok(defaults)
    }

    fn analyze_default_value(
        &mut self,
        target: hir::SourceId,
        table: &Table,
        column: hir::TargetColumn,
    ) -> Result<hir::Expr> {
        let syntax = match column {
            hir::TargetColumn::RowId => ast::Expr::Literal(ast::Literal::Null),
            hir::TargetColumn::Column(column) => {
                let definition = table.columns().get(column).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "INSERT target column {column} is outside table {}",
                        table.get_name()
                    ))
                })?;
                match &definition.default {
                    Some(default) => default.as_ref().clone(),
                    None => {
                        let type_default = self
                            .context()
                            .main_schema()
                            .resolve_type(&definition.ty_str, table.is_strict())?
                            .and_then(|resolved| resolved.default_expr().cloned());
                        type_default.unwrap_or(ast::Expr::Literal(ast::Literal::Null))
                    }
                }
            }
        };
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing INSERT target source {target}"))
        })?;
        let mut scope = Scope::default();
        scope.add_source(source, true);
        self.analyze_expr(
            &syntax,
            &scope,
            ExprPolicy::schema_expression().with_self_source(target),
        )
    }
}

fn simple_values_rows(select: &ast::Select) -> Result<&[Vec<Box<ast::Expr>>]> {
    if select.with.is_some()
        || !select.body.compounds.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
    {
        return unsupported_insert("compound or decorated VALUES sources");
    }
    let ast::OneSelect::Values(rows) = &select.body.select else {
        return unsupported_insert("INSERT-SELECT sources");
    };
    Ok(rows)
}

fn resolve_insert_targets(table: &Table, names: &[ast::Name]) -> Result<Vec<hir::InsertTarget>> {
    if names.is_empty() {
        return Ok(table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| !column.hidden() && !column.is_generated())
            .map(|(column, _)| hir::InsertTarget {
                column: hir::TargetColumn::Column(column),
                uses_value: true,
            })
            .collect());
    }

    let mut targets = Vec::<hir::InsertTarget>::with_capacity(names.len());
    for name in names {
        let normalized = normalize_ident(name.as_str());
        let column = if let Some((column, definition)) = table.get_column_by_name(&normalized) {
            definition.ensure_not_generated("INSERT into", &normalized)?;
            hir::TargetColumn::Column(column)
        } else if is_rowid_name(&normalized) && table_has_rowid(table) {
            table
                .columns()
                .iter()
                .position(|column| column.is_rowid_alias())
                .map_or(hir::TargetColumn::RowId, hir::TargetColumn::Column)
        } else {
            crate::bail_parse_error!(
                "table {} has no column named {}",
                table.get_name(),
                normalized
            );
        };
        let rowid = is_rowid_target(table, column);
        let duplicate = targets
            .iter_mut()
            .filter(|target| target.column == column)
            .last();
        let uses_value = match duplicate {
            Some(previous) if rowid => {
                previous.uses_value = false;
                true
            }
            Some(_) => false,
            None => true,
        };
        targets.push(hir::InsertTarget { column, uses_value });
    }
    Ok(targets)
}

fn is_rowid_target(table: &Table, target: hir::TargetColumn) -> bool {
    match target {
        hir::TargetColumn::RowId => true,
        hir::TargetColumn::Column(column) => table
            .columns()
            .get(column)
            .is_some_and(|column| column.is_rowid_alias()),
    }
}

fn table_has_rowid(table: &Table) -> bool {
    table.btree().is_some_and(|table| table.has_rowid)
}

fn is_rowid_name(name: &str) -> bool {
    ["rowid", "_rowid_", "oid"]
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(name))
}

fn expression_contains_subquery(expression: &ast::Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expression, &mut |expression| {
        if matches!(
            expression,
            ast::Expr::Subquery(_)
                | ast::Expr::Exists(_)
                | ast::Expr::InSelect { .. }
                | ast::Expr::InTable { .. }
        ) {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    });
    found
}

fn unsupported_insert<T>(feature: &str) -> Result<T> {
    Err(LimboError::ParseError(format!(
        "semantic INSERT does not yet accept {feature}"
    )))
}
