//! Basic DELETE conversion into closed semantic HIR.

use turso_parser::ast;

use super::{
    analyze::Analyzer,
    expr::ExprPolicy,
    hir::{self, HirRoot, SourceOwner},
    scope::Scope,
};
use crate::{LimboError, Result};

impl<'ast> Analyzer<'_, '_, 'ast> {
    pub(super) fn analyze_delete(
        &mut self,
        with: Option<&'ast ast::With>,
        table_name: &'ast ast::QualifiedName,
        indexed: Option<&'ast ast::Indexed>,
        where_clause: Option<&'ast ast::Expr>,
        returning: &'ast [ast::ResultColumn],
    ) -> Result<HirRoot> {
        reject_deferred_delete_clauses(with, returning)?;

        let target =
            self.analyze_base_table_source(table_name, None, indexed, SourceOwner::Root)?;
        let table = match &self
            .source(target)
            .ok_or_else(|| LimboError::InternalError(format!("missing DELETE target {target}")))?
            .kind
        {
            hir::SourceKind::Table(table) => table.clone(),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "DELETE target {target} is not a catalog table"
                )));
            }
        };
        let btree = table.value().btree().ok_or_else(|| {
            LimboError::ParseError("semantic DELETE does not yet accept virtual tables".to_string())
        })?;
        if !btree.has_rowid {
            return Err(LimboError::ParseError(
                "DELETE from WITHOUT ROWID tables is not supported".to_string(),
            ));
        }
        self.reject_delete_write_sidecars(table.value())?;
        self.analyze_btree_delete_metadata(target, &table)?;

        let scope = self.delete_read_scope(target)?;
        let predicate = where_clause
            .map(|syntax| {
                self.analyze_root_expr(
                    syntax,
                    &scope,
                    ExprPolicy::where_clause(self.context().dqs_dml()),
                )
                .map(|resolved| resolved.expr)
            })
            .transpose()?;

        Ok(HirRoot::Delete(hir::Delete {
            target,
            target_kind: hir::DeleteTargetKind::BTree {
                triggers: Vec::new(),
                foreign_keys: hir::DmlForeignKeys::default(),
            },
            predicate,
            order_by: Vec::new(),
            limit: None,
            returning: None,
            trigger: None,
        }))
    }

    fn delete_read_scope(&self, target: hir::SourceId) -> Result<Scope> {
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing DELETE target source {target}"))
        })?;
        let mut scope = Scope::default();
        scope.add_source(source, true);
        Ok(scope)
    }

    fn reject_delete_write_sidecars(&self, table: &crate::schema::Table) -> Result<()> {
        let name = table.get_name();
        let schema = self.context().main_schema();
        if schema.get_triggers_for_table(name).next().is_some() {
            return unsupported_delete("targets with triggers");
        }
        if schema.has_child_fks(name) || schema.any_resolved_fks_referencing(name) {
            return unsupported_delete("targets with foreign keys");
        }
        Ok(())
    }
}

fn reject_deferred_delete_clauses(
    with: Option<&ast::With>,
    returning: &[ast::ResultColumn],
) -> Result<()> {
    if with.is_some() {
        return unsupported_delete("WITH clauses");
    }
    if !returning.is_empty() {
        return unsupported_delete("RETURNING clauses");
    }
    Ok(())
}

fn unsupported_delete<T>(feature: &str) -> Result<T> {
    Err(LimboError::ParseError(format!(
        "semantic DELETE does not yet support {feature}"
    )))
}
