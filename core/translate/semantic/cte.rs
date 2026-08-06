//! Lazy common-table-expression analysis.

use turso_parser::ast;

use super::{
    analyze::Analyzer,
    hir::{Cte, CteBody, CteColumn, CteId},
};
use crate::{LimboError, Result};

pub(super) struct CteScope<'ast> {
    entries: Vec<PendingCte<'ast>>,
}

struct PendingCte<'ast> {
    name: String,
    syntax: &'ast ast::CommonTableExpr,
    state: CteState,
}

enum CteState {
    Unbound,
    Binding,
    Bound(CteId),
    Failed(String),
}

impl<'context, 'catalog, 'ast> Analyzer<'context, 'catalog, 'ast> {
    pub(super) fn push_cte_scope(&mut self, with: &'ast ast::With) -> Result<()> {
        let mut entries = Vec::with_capacity(with.ctes.len());
        for cte in &with.ctes {
            let name = crate::util::normalize_ident(cte.tbl_name.as_str());
            if entries
                .iter()
                .any(|entry: &PendingCte<'_>| entry.name == name)
            {
                crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
            }
            entries.push(PendingCte {
                name,
                syntax: cte,
                state: CteState::Unbound,
            });
        }
        self.cte_scopes.push(CteScope { entries });
        Ok(())
    }

    pub(super) fn resolve_cte(&mut self, name: &str) -> Result<Option<CteId>> {
        let name = crate::util::normalize_ident(name);
        let Some((scope, entry)) =
            self.cte_scopes
                .iter()
                .enumerate()
                .rev()
                .find_map(|(scope, entries)| {
                    entries
                        .entries
                        .iter()
                        .position(|entry| entry.name == name)
                        .map(|entry| (scope, entry))
                })
        else {
            return Ok(None);
        };

        match &self.cte_scopes[scope].entries[entry].state {
            CteState::Bound(id) => return Ok(Some(*id)),
            CteState::Binding => crate::bail_parse_error!("circular reference: {}", name),
            CteState::Failed(message) => {
                return Err(LimboError::ParseError(message.clone()));
            }
            CteState::Unbound => {}
        }

        self.cte_scopes[scope].entries[entry].state = CteState::Binding;
        let syntax = self.cte_scopes[scope].entries[entry].syntax;
        let result = self.bind_cte(syntax);
        match result {
            Ok(id) => {
                self.cte_scopes[scope].entries[entry].state = CteState::Bound(id);
                Ok(Some(id))
            }
            Err(error) => {
                let message = match error {
                    LimboError::ParseError(message) => message,
                    other => other.to_string(),
                };
                self.cte_scopes[scope].entries[entry].state = CteState::Failed(message.clone());
                Err(LimboError::ParseError(message))
            }
        }
    }

    fn bind_cte(&mut self, syntax: &'ast ast::CommonTableExpr) -> Result<CteId> {
        let query = self.analyze_select(&syntax.select)?;
        let query = self
            .query(query)
            .ok_or_else(|| LimboError::InternalError("missing bound CTE query".to_string()))?;
        let first = &query.blocks[0];

        if !syntax.columns.is_empty() && syntax.columns.len() != first.outputs.len() {
            crate::bail_parse_error!(
                "table {} has {} values for {} columns",
                syntax.tbl_name.as_str(),
                first.outputs.len(),
                syntax.columns.len()
            );
        }

        let columns = first
            .outputs
            .iter()
            .enumerate()
            .map(|(index, output)| CteColumn {
                name: syntax
                    .columns
                    .get(index)
                    .map(|column| crate::util::normalize_ident(column.col_name.as_str()))
                    .unwrap_or_else(|| output.name.clone()),
                type_fact: output.type_fact.clone(),
                affinity: output.schema_affinity,
                has_affinity: output.has_affinity,
                collation: output.collation.clone(),
            })
            .collect();
        let query = query.id;
        // Allocate only referenced definitions. Closed HIR rejects arena
        // entries that cannot be reached from the statement root.
        let id = self.reserve_cte();
        self.insert_cte(
            id,
            Cte {
                id,
                name: crate::util::normalize_ident(syntax.tbl_name.as_str()),
                columns,
                materialized: syntax.materialized.clone(),
                body: CteBody::Query(query),
            },
        )?;
        Ok(id)
    }
}
