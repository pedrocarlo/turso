//! INSERT conversion into closed semantic HIR.

use turso_parser::ast;

use super::{
    analyze::{output_from_resolved, Analyzer, CatalogObjectKind},
    expr::ExprPolicy,
    hir::{self, HirRoot, SourceOwner},
    scope::Scope,
};
use crate::{
    schema::{Table, TypeDef},
    sync::Arc,
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

        let (columns, source, upserts) = match body {
            ast::InsertBody::DefaultValues => {
                if !column_names.is_empty() {
                    return unsupported_insert("a column list with DEFAULT VALUES");
                }
                (Vec::new(), hir::InsertSource::DefaultValues, Vec::new())
            }
            ast::InsertBody::Select(select, upsert) => {
                let columns = resolve_insert_targets(table.value(), column_names)?;
                let expected = columns.len();
                let source = match &select.body.select {
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
                        hir::InsertSource::Values(bound_rows)
                    }
                    ast::OneSelect::Select { .. } => {
                        let expected_outputs = columns
                            .iter()
                            .map(|target| self.insert_target_type(table.value(), target.column))
                            .collect::<Result<Vec<_>>>()?;
                        let query = self.analyze_insert_select(select, with, &expected_outputs)?;
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
                        hir::InsertSource::Query(query)
                    }
                };
                let upserts = analyze_catch_all_upserts(upsert.as_deref())?;
                (columns, source, upserts)
            }
        };
        let defaults = self.analyze_insert_defaults(target, table.value(), &columns)?;
        let returning = self.analyze_insert_returning(returning, target);
        if with.is_some() {
            self.cte_scopes
                .pop()
                .expect("an INSERT WITH clause must own one CTE scope");
        }
        let returning = returning?;

        Ok(HirRoot::Insert(hir::Insert {
            target,
            autoincrement: None,
            autoincrement_sequence: None,
            columns,
            defaults,
            source,
            conflict,
            upserts,
            excluded_source: None,
            returning,
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
        expected_outputs: &[Option<Arc<TypeDef>>],
    ) -> Result<hir::QueryId> {
        if let Some(with) = with {
            self.push_cte_scope(with)?;
        }
        self.analyze_select_with_expected_outputs(select, expected_outputs)
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
        self.analyze_expr_with_expected_type(
            syntax,
            &Scope::default(),
            ExprPolicy::insert_values(self.context().dqs_dml()),
            self.insert_target_type(table, column)?,
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
        self.analyze_expr_with_expected_type(
            &syntax,
            &scope,
            ExprPolicy::schema_expression().with_self_source(target),
            self.insert_target_type(table, column)?,
        )
    }

    fn insert_target_type(
        &self,
        table: &Table,
        column: hir::TargetColumn,
    ) -> Result<Option<Arc<TypeDef>>> {
        let hir::TargetColumn::Column(column) = column else {
            return Ok(None);
        };
        let definition = table.columns().get(column).ok_or_else(|| {
            LimboError::InternalError(format!(
                "INSERT target column {column} is outside table {}",
                table.get_name()
            ))
        })?;
        Ok(self
            .context()
            .main_schema()
            .get_type_def_unchecked(&definition.ty_str)
            .cloned())
    }

    fn analyze_insert_returning(
        &mut self,
        columns: &'ast [ast::ResultColumn],
        target: hir::SourceId,
    ) -> Result<Option<hir::Returning>> {
        if columns.is_empty() {
            return Ok(None);
        }
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing INSERT target source {target}"))
        })?;
        let mut scope = Scope::default();
        scope.add_source(source, true);
        let policy = ExprPolicy::returning(self.context().dqs_dml());
        let mut outputs = Vec::with_capacity(columns.len());

        for column in columns {
            match column {
                ast::ResultColumn::Expr(expression, alias) => {
                    let resolved = self.analyze_root_expr(expression, &scope, policy)?;
                    let (name, name_kind) = match alias {
                        Some(alias) if alias.is_explicit() => (
                            alias.name().as_str().to_string(),
                            hir::OutputNameKind::ExplicitAlias,
                        ),
                        Some(ast::As::ImplicitColumnName(name)) => {
                            (name.as_str().to_string(), hir::OutputNameKind::Inferred)
                        }
                        None => (expression.to_string(), hir::OutputNameKind::Inferred),
                        Some(_) => unreachable!("all explicit aliases were handled"),
                    };
                    outputs.push(output_from_resolved(
                        hir::OutputId::root(outputs.len()),
                        name,
                        name_kind,
                        resolved,
                    ));
                }
                ast::ResultColumn::Star => {
                    let expanded = scope.expand_star()?;
                    self.append_returning_star_outputs(&mut outputs, expanded, &scope)?;
                }
                ast::ResultColumn::TableStar(table) => {
                    let expanded = scope.expand_table_star(table.as_str())?;
                    self.append_returning_star_outputs(&mut outputs, expanded, &scope)?;
                }
            }
        }
        Ok(Some(hir::Returning { outputs }))
    }

    fn append_returning_star_outputs(
        &self,
        outputs: &mut Vec<hir::Output>,
        expanded: Vec<super::scope::ExpandedColumn>,
        scope: &Scope,
    ) -> Result<()> {
        for column in expanded {
            let resolved = self.resolve_atomic_expr(column.resolved.expr, scope)?;
            outputs.push(output_from_resolved(
                hir::OutputId::root(outputs.len()),
                column.name,
                hir::OutputNameKind::StarExpansion,
                resolved,
            ));
        }
        Ok(())
    }
}

fn analyze_catch_all_upserts(mut syntax: Option<&ast::Upsert>) -> Result<Vec<hir::Upsert>> {
    let mut upserts = Vec::new();
    while let Some(upsert) = syntax {
        if upsert.index.is_some() {
            return unsupported_insert("UPSERT conflict targets");
        }
        if matches!(upsert.do_clause, ast::UpsertDo::Set { .. }) {
            return unsupported_insert("UPSERT DO UPDATE clauses");
        }
        upserts.push(hir::Upsert {
            target: None,
            action: hir::UpsertAction::Nothing,
        });
        syntax = upsert.next.as_deref();
    }
    Ok(upserts)
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
