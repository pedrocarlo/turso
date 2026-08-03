use crate::alloc::TursoVecExt;
use crate::function::{Deterministic, Func, MathFunc, ScalarFunc};
use crate::sync::Arc;
use crate::vdbe::builder::ProgramBuilder;

use rustc_hash::FxHashMap as HashMap;
use smallvec::SmallVec;
use std::cell::RefCell;
use std::num::NonZero;
use turso_parser::ast::{self, JoinConstraint, SortOrder, SortedColumn, TableInternalId};

use super::emitter::Resolver;
use super::expr::{unwrap_parens, walk_expr, walk_expr_mut, WalkControl};
use super::plan::{BitSet, ColumnMask, JoinInfo, TableReferences};
use super::planner::parse_row_id;
use crate::schema::{
    is_deterministic_schema_function_call, BTreeTable, Column, GeneratedType, Index, IndexColumn,
    Schema, Table, TypeDef, EXPR_INDEX_SENTINEL,
};
use crate::util::normalize_ident;
use crate::Result;

/// Take ownership of an expression, leaving a NULL literal in its place.
/// The caller is expected to overwrite the original slot immediately after.
fn take_expr(expr: &mut ast::Expr) -> ast::Expr {
    std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
}

fn rewrite_between_node(expr: &mut ast::Expr) -> bool {
    let ast::Expr::Between {
        lhs,
        not,
        start,
        end,
    } = expr
    else {
        return false;
    };
    let lhs = take_expr(lhs);
    let start = take_expr(start);
    let end = take_expr(end);
    let (lower, upper, combine) = if *not {
        (
            ast::Expr::Binary(Box::new(lhs.clone()), ast::Operator::Less, Box::new(start)),
            ast::Expr::Binary(Box::new(lhs), ast::Operator::Greater, Box::new(end)),
            ast::Operator::Or,
        )
    } else {
        (
            ast::Expr::Binary(
                Box::new(lhs.clone()),
                ast::Operator::GreaterEquals,
                Box::new(start),
            ),
            ast::Expr::Binary(Box::new(lhs), ast::Operator::LessEquals, Box::new(end)),
            ast::Operator::And,
        )
    };
    *expr = ast::Expr::Binary(Box::new(lower), combine, Box::new(upper));
    true
}

fn rewrite_between_expressions(expr: &mut ast::Expr) {
    let _ = walk_expr_mut(expr, &mut |expr| {
        rewrite_between_node(expr);
        Ok(WalkControl::Continue)
    });
}

/// Bind the synthetic `value` name in a domain CHECK to its concrete column.
pub fn bind_domain_check(expr: &ast::Expr, column_name: &str) -> Box<ast::Expr> {
    let mut bound = expr.clone();
    let _ = walk_expr_mut(&mut bound, &mut |expr| {
        if let ast::Expr::Id(name) = expr {
            if name.as_str().eq_ignore_ascii_case("value") {
                *expr = ast::Expr::Id(ast::Name::exact(column_name.to_string()));
            }
        }
        Ok(WalkControl::Continue)
    });
    Box::new(bound)
}

/// Shift stored generated-column bindings after a table column is removed.
pub fn shift_generated_columns_after_drop(
    table: &mut BTreeTable,
    dropped_column: usize,
) -> Result<()> {
    if !table.has_virtual_columns {
        return Ok(());
    }

    let mut columns = table.columns_mut();
    for column in columns.iter_mut() {
        let Some(expr) = column.generated_expr_mut() else {
            continue;
        };
        shift_schema_expr_after_drop(expr, dropped_column, true)?;
    }
    Ok(())
}

/// Shift a stored schema expression's bound column positions after DROP COLUMN.
pub fn shift_schema_expr_after_drop(
    expr: &mut ast::Expr,
    dropped_column: usize,
    reject_dropped_reference: bool,
) -> Result<()> {
    walk_expr_mut(expr, &mut |expr| {
        if let ast::Expr::Column { table, column, .. } = expr {
            if table.is_self_table() {
                if reject_dropped_reference && *column == dropped_column {
                    return Err(crate::LimboError::InternalError(
                        "dropped column remained referenced by generated column".to_string(),
                    ));
                }
                if *column > dropped_column {
                    *column -= 1;
                }
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Shift an index's bound keys and predicate after DROP COLUMN.
pub fn shift_index_after_drop(index: &mut Index, dropped_column: usize) -> Result<()> {
    for index_column in index.columns.iter_mut() {
        if index_column.pos_in_table != EXPR_INDEX_SENTINEL
            && index_column.pos_in_table > dropped_column
        {
            index_column.pos_in_table -= 1;
        }
        if let Some(expr) = &mut index_column.expr {
            shift_schema_expr_after_drop(expr, dropped_column, false)?;
        }
    }
    if let Some(predicate) = &mut index.where_clause {
        shift_schema_expr_after_drop(predicate, dropped_column, false)?;
    }
    Ok(())
}

/// Render a bound schema expression using the table's current column names.
pub fn render_schema_expr(expr: &ast::Expr, columns: &[Column]) -> Result<String> {
    let mut unbound = expr.clone();
    walk_expr_mut(&mut unbound, &mut |expr| {
        match expr {
            ast::Expr::Column { table, column, .. } if table.is_self_table() => {
                if let Some(name) = columns.get(*column).and_then(|column| column.name.as_ref()) {
                    *expr = ast::Expr::Id(ast::Name::exact(name.clone()));
                }
            }
            ast::Expr::RowId { table, .. } if table.is_self_table() => {
                *expr = ast::Expr::Id(ast::Name::exact(super::planner::ROWID_STRS[0].to_string()));
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(unbound.to_string())
}

/// Rename raw column identifiers left in a leniently loaded schema expression.
pub fn rename_schema_expr_identifiers(expr: &mut ast::Expr, from: &str, to: &str) {
    let _ = walk_expr_mut(expr, &mut |expr| {
        match expr {
            ast::Expr::Id(name) | ast::Expr::Name(name)
                if name.as_str().eq_ignore_ascii_case(from) =>
            {
                *expr = ast::Expr::Id(ast::Name::exact(to.to_owned()));
            }
            ast::Expr::Qualified(table, column) if column.as_str().eq_ignore_ascii_case(from) => {
                *expr = ast::Expr::Qualified(table.clone(), ast::Name::exact(to.to_owned()));
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
}

/// Substitute one table column in a CHECK expression with its ADD COLUMN default.
pub fn bind_check_column_default(
    expr: &ast::Expr,
    table_name: &str,
    column_name: &str,
    default_expr: &ast::Expr,
) -> ast::Expr {
    let normalized_table = normalize_ident(table_name);
    let normalized_column = normalize_ident(column_name);
    let mut bound = expr.clone();
    let _ = walk_expr_mut(&mut bound, &mut |expr| match expr {
        ast::Expr::Id(name) if normalize_ident(name.as_str()) == normalized_column => {
            *expr = default_expr.clone();
            Ok(WalkControl::SkipChildren)
        }
        ast::Expr::Qualified(table, column)
            if normalize_ident(table.as_str()) == normalized_table
                && normalize_ident(column.as_str()) == normalized_column =>
        {
            *expr = default_expr.clone();
            Ok(WalkControl::SkipChildren)
        }
        _ => Ok(WalkControl::Continue),
    });
    bound
}

/// Reject a column rename when a direct reference to the renamed table
/// participates in a JOIN USING the old column name.
///
/// A USING name identifies columns on both sides of the join. Rewriting the
/// unresolved name would guess which side owns it, while leaving it unchanged
/// would make the renamed table fail the join. Rejecting the rename keeps us
/// from persisting a schema whose column identity was guessed.
pub fn validate_column_rename_using_clause(
    from: &Option<ast::FromClause>,
    target_table: &str,
    old_column: &str,
) -> Result<()> {
    let Some(from) = from else {
        return Ok(());
    };

    let is_target = |table: &ast::SelectTable| {
        matches!(
            table,
            ast::SelectTable::Table(name, _, _)
                if name.name.as_str().eq_ignore_ascii_case(target_table)
        )
    };

    let mut target_is_on_left = is_target(&from.select);
    for join in &from.joins {
        let target_is_on_right = is_target(&join.table);
        let uses_old_column = matches!(
            &join.constraint,
            Some(ast::JoinConstraint::Using(columns))
                if columns
                    .iter()
                    .any(|column| column.as_str().eq_ignore_ascii_case(old_column))
        );
        if uses_old_column && (target_is_on_left || target_is_on_right) {
            crate::bail_parse_error!(
                "cannot join using column {} - column not present in both tables",
                old_column
            );
        }
        target_is_on_left |= target_is_on_right;
    }

    Ok(())
}

/// Point SELF_TABLE references in a stored schema expression at a concrete
/// table reference. This does not reject unresolved identifiers because ALTER
/// validation may deliberately bind them through an expression register cache.
pub fn rebase_schema_expr(expr: &mut ast::Expr, internal_id: ast::TableInternalId) {
    let _ = walk_expr_mut(expr, &mut |expr| {
        match expr {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. }
                if table.is_self_table() =>
            {
                *table = internal_id;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
}

/// Clone a stored schema expression and bind its SELF_TABLE references to a
/// concrete table reference.
pub fn bind_schema_expr(expr: &ast::Expr, internal_id: ast::TableInternalId) -> Result<ast::Expr> {
    let mut bound = expr.clone();
    if let Some(name) = first_unbound_identifier(&bound) {
        crate::bail_parse_error!("no such column: {}", name);
    }
    rebase_schema_expr(&mut bound, internal_id);
    Ok(bound)
}

/// Bind every generated-column name to a SELF_TABLE column position.
pub fn bind_generated_column_expr(expr: &mut ast::Expr, columns: &[Column]) -> Result<()> {
    walk_expr_mut(expr, &mut |expr| match expr {
        ast::Expr::Id(name)
        | ast::Expr::Qualified(_, name)
        | ast::Expr::DoublyQualified(_, _, name) => {
            let column_name = normalize_ident(name.as_str());
            let (column, definition) = columns
                .iter()
                .enumerate()
                .find(|(_, column)| {
                    column
                        .name
                        .as_ref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&column_name))
                })
                .ok_or_else(|| {
                    crate::LimboError::ParseError(format!("no such column: {column_name}"))
                })?;
            *expr = ast::Expr::Column {
                database: None,
                table: ast::TableInternalId::SELF_TABLE,
                column,
                is_rowid_alias: definition.is_rowid_alias(),
            };
            Ok(WalkControl::Continue)
        }
        _ => Ok(WalkControl::Continue),
    })?;
    Ok(())
}

/// Resolve a generated expression's column dependencies to table positions.
pub fn bind_generated_column_dependencies(
    expr: &ast::Expr,
    columns: &[Column],
    dependencies: &mut BitSet,
) -> Result<()> {
    walk_expr(expr, &mut |expr| {
        match expr {
            ast::Expr::Column { table, column, .. } if table.is_self_table() => {
                dependencies.set(*column)?;
            }
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                if let Some(column) = columns.iter().position(|column| {
                    column
                        .name
                        .as_ref()
                        .is_some_and(|column_name| column_name.eq_ignore_ascii_case(name.as_str()))
                }) {
                    dependencies.set(column)?;
                }
            }
            ast::Expr::Qualified(_, name) | ast::Expr::DoublyQualified(_, _, name) => {
                if let Some(column) = columns.iter().position(|column| {
                    column
                        .name
                        .as_ref()
                        .is_some_and(|column_name| column_name.eq_ignore_ascii_case(name.as_str()))
                }) {
                    dependencies.set(column)?;
                }
            }
            ast::Expr::Subquery(_)
            | ast::Expr::Exists(_)
            | ast::Expr::InTable { .. }
            | ast::Expr::SubqueryResult { .. } => {
                unreachable!("generated columns cannot contain subqueries")
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Resolve columns read by a stored index expression to table positions.
pub fn bind_index_expression_columns(
    table: &Table,
    columns: &mut ColumnMask,
    expr: &ast::Expr,
) -> Result<()> {
    walk_expr(expr, &mut |expr| {
        match expr {
            ast::Expr::Id(name) => {
                if let Some((column, _)) = table.get_column_by_name(&normalize_ident(name.as_str()))
                {
                    columns.set(column)?;
                } else if super::planner::ROWID_STRS
                    .iter()
                    .any(|rowid| rowid.eq_ignore_ascii_case(name.as_str()))
                {
                    if let Some(rowid_column) = table
                        .btree()
                        .and_then(|table| table.get_rowid_alias_column().map(|(column, _)| column))
                    {
                        columns.set(rowid_column)?;
                    }
                }
            }
            ast::Expr::Qualified(namespace, name)
            | ast::Expr::DoublyQualified(_, namespace, name) => {
                if normalize_ident(namespace.as_str())
                    .eq_ignore_ascii_case(&normalize_ident(table.get_name()))
                {
                    if let Some((column, _)) =
                        table.get_column_by_name(&normalize_ident(name.as_str()))
                    {
                        columns.set(column)?;
                    }
                }
            }
            ast::Expr::Column { column, .. } => columns.set(*column)?,
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Bind index keys, partial-index predicates, and CHECK expressions to
/// SELF_TABLE positions. Unknown names remain unbound so stale schema entries
/// can load and fail when the expression is used.
pub fn bind_index_schema_expr(expr: &mut ast::Expr, table: &BTreeTable) {
    let table_name = normalize_ident(table.name.as_str());
    let _ = walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        let resolved = match expr {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                bind_self_table_leaf(&normalize_ident(name.as_str()), table)
            }
            ast::Expr::Qualified(namespace, column)
            | ast::Expr::DoublyQualified(_, namespace, column)
                if normalize_ident(namespace.as_str()).eq_ignore_ascii_case(&table_name) =>
            {
                bind_self_table_leaf(&normalize_ident(column.as_str()), table)
            }
            _ => None,
        };
        if let Some(resolved) = resolved {
            *expr = resolved;
        }
        Ok(WalkControl::Continue)
    });
}

/// Bind CREATE INDEX key expressions to columns of the indexed table.
pub fn bind_index_columns(
    table: &BTreeTable,
    columns: &[SortedColumn],
    resolver: Option<&Resolver>,
) -> Result<crate::alloc::Vec<IndexColumn>> {
    super::index::reject_explicit_nulls(columns)?;
    let mut bound =
        <crate::alloc::Vec<_> as crate::alloc::TursoTryWithCapacityExt>::try_with_capacity_ext(
            columns.len(),
        )?;
    for sorted_column in columns {
        let order = sorted_column.order.unwrap_or(SortOrder::Asc);
        let (explicit_collation, base_expr) =
            extract_index_collation(sorted_column.expr.as_ref(), resolver)?;
        let unwrapped_expr = unwrap_parens(base_expr)?;
        if let Some((position, column_name, column)) = bind_index_column(unwrapped_expr, table) {
            let collation = explicit_collation.or_else(|| column.collation_opt());
            let expr = match column.generated_type() {
                GeneratedType::Virtual { expr, .. } => Some(expr.clone()),
                GeneratedType::NotGenerated => None,
            };
            bound
                .push_within_capacity(IndexColumn {
                    name: column_name,
                    order,
                    pos_in_table: position,
                    collation,
                    default: column.default.clone(),
                    expr,
                })
                .expect("bound index columns vector was preallocated to columns.len()");
            continue;
        }
        if !is_valid_index_expression(unwrapped_expr, table) {
            crate::bail_parse_error!(
                "Error: invalid expression in CREATE INDEX: {}",
                sorted_column.expr
            );
        }
        let mut key_expr = sorted_column.expr.clone();
        bind_index_schema_expr(&mut key_expr, table);
        bound
            .push_within_capacity(IndexColumn {
                name: sorted_column.expr.to_string(),
                order,
                pos_in_table: EXPR_INDEX_SENTINEL,
                collation: explicit_collation,
                default: None,
                expr: Some(key_expr),
            })
            .expect("bound index columns vector was preallocated to columns.len()");
    }
    Ok(bound)
}

/// Validate that a partial-index predicate only refers to its indexed table.
pub fn validate_partial_index_predicate(index: &Index, table: &Table) -> bool {
    let Some(predicate) = &index.where_clause else {
        return true;
    };

    let has_column = |name: &str| {
        table.columns().iter().any(|column| {
            column
                .name
                .as_ref()
                .is_some_and(|column_name| column_name.eq_ignore_ascii_case(name))
        })
    };
    let is_table = |name: &str| normalize_ident(name) == index.table_name;
    let is_deterministic_function = |name: &str, arg_count: usize| {
        let name = normalize_ident(name);
        Func::resolve_function(&name, arg_count)
            .is_ok_and(|function| function.is_some_and(|function| function.is_deterministic()))
    };

    let mut valid = true;
    let _ = walk_expr(
        predicate.as_ref(),
        &mut |expr: &ast::Expr| -> Result<WalkControl> {
            if !valid {
                return Ok(WalkControl::SkipChildren);
            }
            match expr {
                ast::Expr::Literal(_) | ast::Expr::RowId { .. } => {}
                ast::Expr::Id(name) => {
                    if !super::planner::ROWID_STRS
                        .iter()
                        .any(|rowid| rowid.eq_ignore_ascii_case(name.as_str()))
                        && !has_column(name.as_str())
                    {
                        valid = false;
                    }
                }
                ast::Expr::Qualified(namespace, column)
                | ast::Expr::DoublyQualified(_, namespace, column) => {
                    if !is_table(namespace.as_str()) || !has_column(column.as_str()) {
                        valid = false;
                    }
                }
                ast::Expr::FunctionCall {
                    name, filter_over, ..
                }
                | ast::Expr::FunctionCallStar {
                    name, filter_over, ..
                } => {
                    if filter_over.over_clause.is_some() {
                        valid = false;
                    } else {
                        let arg_count = match expr {
                            ast::Expr::FunctionCall { args, .. } => args.len(),
                            ast::Expr::FunctionCallStar { .. } => 0,
                            _ => unreachable!(),
                        };
                        if !is_deterministic_function(name.as_str(), arg_count) {
                            valid = false;
                        }
                    }
                }
                ast::Expr::Exists(_)
                | ast::Expr::InSelect { .. }
                | ast::Expr::Subquery(_)
                | ast::Expr::Raise { .. }
                | ast::Expr::Variable(_) => valid = false,
                _ => {}
            }
            Ok(if valid {
                WalkControl::Continue
            } else {
                WalkControl::SkipChildren
            })
        },
    );
    valid
}

/// Bind and validate a CHECK constraint against its table columns.
