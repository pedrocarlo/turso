//! Basic UPDATE conversion into closed semantic HIR.

use turso_parser::ast;

use super::{
    analyze::Analyzer,
    dml::{trigger_matches_update, trigger_targets_database},
    expr::ExprPolicy,
    hir::{self, HirRoot, SourceOwner},
    query::FromContext,
    scope::Scope,
};
use crate::{function::ScalarFunc, schema::Table, util::normalize_ident, LimboError, Result};

impl<'ast> Analyzer<'_, '_, 'ast> {
    pub(super) fn analyze_update(&mut self, syntax: &'ast ast::Update) -> Result<HirRoot> {
        self.with_cte_scope(syntax.with.as_ref(), |analyzer| {
            analyzer.analyze_update_body(syntax)
        })
    }

    fn analyze_update_body(&mut self, syntax: &'ast ast::Update) -> Result<HirRoot> {
        let target = self.analyze_base_table_source(
            &syntax.tbl_name,
            None,
            syntax.indexed.as_ref(),
            SourceOwner::Root,
        )?;
        let table = match &self
            .source(target)
            .ok_or_else(|| LimboError::InternalError(format!("missing UPDATE target {target}")))?
            .kind
        {
            hir::SourceKind::Table(table) => table.clone(),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "UPDATE target {target} is not a catalog table"
                )));
            }
        };
        let virtual_target = table.value().virtual_table().is_some();
        if table.value().btree().is_some_and(|btree| !btree.has_rowid) {
            return Err(LimboError::ParseError(
                "UPDATE of WITHOUT ROWID tables is not supported".to_string(),
            ));
        }
        let new_source = self.create_update_new_source(target, &table)?;
        if !virtual_target {
            self.analyze_btree_write_metadata(target, &table)?;
            self.analyze_btree_write_metadata(new_source, &table)?;
        }

        let (from, from_scope) = match syntax.from.as_ref() {
            Some(syntax) => {
                let (from, scope) = self.analyze_from_clause(syntax, FromContext::Root)?;
                (Some(from), Some(scope))
            }
            None => (None, None),
        };
        let mut scope = self.update_read_scope(target)?;
        if let Some(from_scope) = from_scope {
            scope.append_local(from_scope);
        }
        let assignments =
            self.analyze_update_assignments(&syntax.sets, new_source, table.value(), &scope)?;
        let target_kind = if virtual_target {
            hir::UpdateTargetKind::Virtual
        } else {
            hir::UpdateTargetKind::BTree {
                defaults: Vec::new(),
                triggers: self.analyze_update_triggers(&table, &assignments),
                foreign_keys: self.analyze_dml_foreign_keys(&table, new_source)?,
            }
        };
        let returning = self.analyze_dml_returning(&syntax.returning, new_source)?;
        let predicate = syntax
            .where_clause
            .as_deref()
            .map(|syntax| {
                self.analyze_root_expr(
                    syntax,
                    &scope,
                    ExprPolicy::where_clause(self.context().dqs_dml()),
                )
                .map(|resolved| resolved.expr)
            })
            .transpose()?;

        Ok(HirRoot::Update(hir::Update {
            target,
            new_source,
            target_kind,
            from,
            assignments,
            predicate,
            order_by: Vec::new(),
            limit: None,
            conflict: syntax.or_conflict,
            returning,
            trigger: None,
            cdc_updates_override: None,
        }))
    }

    fn analyze_update_triggers(
        &mut self,
        table: &hir::ResolvedTable,
        assignments: &[hir::Assignment],
    ) -> Vec<hir::ResolvedTrigger> {
        let database = table
            .database()
            .expect("an UPDATE target table must have an owning database");
        let updated_columns = assignments
            .iter()
            .flat_map(|assignment| assignment.columns.iter())
            .filter_map(|column| match column {
                hir::TargetColumn::Column(column) => Some(*column),
                hir::TargetColumn::RowId => None,
            })
            .collect::<Vec<_>>();
        let triggers = self
            .context()
            .main_schema()
            .get_triggers_for_table(table.value().get_name())
            .filter(|trigger| {
                trigger_targets_database(trigger, database)
                    && trigger_matches_update(trigger, table.value(), &updated_columns)
            })
            .cloned()
            .collect::<Vec<_>>();
        triggers
            .into_iter()
            .map(|trigger| self.freeze_trigger(database, trigger))
            .collect()
    }

    fn create_update_new_source(
        &mut self,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
    ) -> Result<hir::SourceId> {
        let old = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing UPDATE target source {target}"))
        })?;
        let columns = old.columns.clone();
        let generated_expressions = old.generated_expressions.clone();
        let default_expressions = old.default_expressions.clone();
        let column_type_programs = old.column_type_programs.clone();
        let rowid_available = old.rowid_available;
        let new_source = self.reserve_source();
        self.insert_source(
            new_source,
            hir::Source {
                id: new_source,
                owner: SourceOwner::Root,
                database: table.database(),
                name: table.value().get_name().to_string(),
                alias: None,
                kind: hir::SourceKind::Pseudo {
                    kind: hir::PseudoSource::New,
                    table: table.clone(),
                },
                columns,
                generated_expressions,
                default_expressions,
                column_type_programs,
                check_constraints: None,
                rowid_available,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(new_source)
    }

    fn update_read_scope(&self, target: hir::SourceId) -> Result<Scope> {
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing UPDATE target source {target}"))
        })?;
        let mut scope = Scope::default();
        scope.add_source(source, true);
        Ok(scope)
    }

    fn analyze_update_assignments(
        &mut self,
        sets: &'ast [ast::Set],
        new_source: hir::SourceId,
        table: &Table,
        scope: &Scope,
    ) -> Result<Vec<hir::Assignment>> {
        let mut assignments = Vec::<hir::Assignment>::new();
        for set in sets {
            let values: Vec<&ast::Expr> = match set.expr.as_ref() {
                ast::Expr::Parenthesized(values) => {
                    if set.col_names.len() != values.len() {
                        crate::bail_parse_error!(
                            "{} columns assigned {} values",
                            set.col_names.len(),
                            values.len()
                        );
                    }
                    values.iter().map(Box::as_ref).collect()
                }
                expression => {
                    if set.col_names.len() != 1 {
                        crate::bail_parse_error!(
                            "{} columns assigned 1 values",
                            set.col_names.len()
                        );
                    }
                    vec![expression]
                }
            };

            for (name, syntax) in set.col_names.iter().zip(values) {
                let column = resolve_update_target(table, name)?;
                let value = if matches!(syntax, ast::Expr::Default) {
                    self.analyze_write_default(new_source, table, column)?
                } else {
                    self.analyze_root_expr_with_expected_type(
                        syntax,
                        scope,
                        ExprPolicy::update(self.context().dqs_dml()),
                        self.write_target_type(table, column)?,
                    )?
                    .expr
                };
                merge_update_assignment(&mut assignments, column, value);
            }
        }
        Ok(assignments)
    }
}

fn resolve_update_target(table: &Table, name: &ast::Name) -> Result<hir::TargetColumn> {
    let normalized = normalize_ident(name.as_str());
    if let Some((column, definition)) = table.get_column_by_name(&normalized) {
        definition.ensure_not_generated("UPDATE", name.as_str())?;
        return Ok(hir::TargetColumn::Column(column));
    }
    if (table.btree().is_some_and(|btree| btree.has_rowid) || table.virtual_table().is_some())
        && ["rowid", "_rowid_", "oid"]
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&normalized))
    {
        return Ok(table
            .columns()
            .iter()
            .position(|column| column.is_rowid_alias())
            .map_or(hir::TargetColumn::RowId, hir::TargetColumn::Column));
    }
    crate::bail_parse_error!("no such column: {}.{}", table.get_name(), name)
}

fn merge_update_assignment(
    assignments: &mut Vec<hir::Assignment>,
    column: hir::TargetColumn,
    mut value: hir::Expr,
) {
    let Some(existing) = assignments
        .iter_mut()
        .find(|assignment| assignment.columns == [column])
    else {
        assignments.push(hir::Assignment {
            columns: vec![column],
            value,
        });
        return;
    };

    if let hir::Expr::Function(call) = &mut value {
        if matches!(
            call.function.value(),
            crate::Func::Scalar(ScalarFunc::ArraySetElement)
        ) {
            if let hir::FunctionArguments::Expressions { values, .. } = &mut call.arguments {
                if values.len() == 3 {
                    values[0] = existing.value.clone();
                }
            }
        }
    }
    existing.value = value;
}
