//! Expression table-ID remapping.
//!
//! After binding assigns one set of `TableInternalId`s and `parse_from`
//! assigns a different set, this module walks all expressions in a
//! `SelectPlan` and replaces binding IDs with planning IDs.

use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, TableInternalId};

use super::bind::BindScope;
use super::expr::{walk_expr_mut, WalkControl};
use super::plan::{SelectPlan, TableReferences};
use crate::Result;

/// Build a mapping from binding-phase table IDs to planning-phase table IDs.
///
/// Matches tables by identifier name between the `BindScope` (binding)
/// and `TableReferences` (planning).
#[allow(dead_code)]
pub fn build_id_remap(
    scope: &BindScope,
    table_references: &TableReferences,
) -> HashMap<TableInternalId, TableInternalId> {
    let mut remap = HashMap::default();
    for scope_table in &scope.tables {
        if let Some(joined) = table_references
            .joined_tables()
            .iter()
            .find(|t| t.identifier == scope_table.identifier)
        {
            if scope_table.internal_id != joined.internal_id {
                remap.insert(scope_table.internal_id, joined.internal_id);
            }
        }
    }
    remap
}

/// Remap table IDs in a single expression.
#[allow(dead_code)]
pub fn remap_expr_table_ids(
    expr: &mut ast::Expr,
    remap: &HashMap<TableInternalId, TableInternalId>,
) {
    if remap.is_empty() {
        return;
    }
    let _ = walk_expr_mut(expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => {
                if let Some(new_id) = remap.get(table) {
                    *table = *new_id;
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
}

/// Remap all expression table IDs in a SelectPlan.
#[allow(dead_code)]
pub fn remap_select_plan_ids(
    plan: &mut SelectPlan,
    remap: &HashMap<TableInternalId, TableInternalId>,
) {
    if remap.is_empty() {
        return;
    }

    // Result columns
    for rc in &mut plan.result_columns {
        remap_expr_table_ids(&mut rc.expr, remap);
    }

    // WHERE clause
    for term in &mut plan.where_clause {
        remap_expr_table_ids(&mut term.expr, remap);
    }

    // GROUP BY
    if let Some(ref mut group_by) = plan.group_by {
        for expr in &mut group_by.exprs {
            remap_expr_table_ids(expr, remap);
        }
        if let Some(ref mut having) = group_by.having {
            for expr in having {
                remap_expr_table_ids(expr, remap);
            }
        }
    }

    // ORDER BY
    for (expr, _) in &mut plan.order_by {
        remap_expr_table_ids(expr, remap);
    }

    // Aggregates
    for agg in &mut plan.aggregates {
        for arg in &mut agg.args {
            remap_expr_table_ids(arg, remap);
        }
        remap_expr_table_ids(&mut agg.original_expr, remap);
    }

    // VALUES
    for row in &mut plan.values {
        for expr in row {
            remap_expr_table_ids(expr, remap);
        }
    }

    // Window
    if let Some(ref mut window) = plan.window {
        for expr in &mut window.partition_by {
            remap_expr_table_ids(expr, remap);
        }
        for (expr, _) in &mut window.order_by {
            remap_expr_table_ids(expr, remap);
        }
        for func in &mut window.functions {
            remap_expr_table_ids(&mut func.original_expr, remap);
        }
    }
}
