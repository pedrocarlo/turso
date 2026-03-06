//! Resolved expression IR.
//!
//! [`ResolvedExpr`] is the intermediate representation produced after the
//! bind-and-rewrite phase. Every column, alias, and table reference has been
//! resolved to concrete indices/IDs, and syntactic sugar (`BETWEEN`,
//! parentheses, bare identifiers) has been eliminated.
//!
//! **Invariants**:
//! - No `Expr::Id`, `Expr::Qualified`, or `Expr::DoublyQualified` survive lowering.
//! - `BETWEEN` is rewritten to `Binary(And/Or)` before lowering.
//! - `Expr::Exists`, `Expr::InSelect`, `Expr::Subquery` are planned into
//!   `SubqueryResult` before lowering.
//! - `Parenthesized` is unwrapped.
//!
//! The IR is consumed by the optimizer and the bytecode emitter.

use turso_parser::ast::{
    self, Distinctness, Expr, LikeOperator, Literal, ResolveType, SubqueryType, UnaryOperator,
};
use turso_parser::ast::{Operator, TableInternalId};

use crate::Result;

// ---------------------------------------------------------------------------
// Core IR type
// ---------------------------------------------------------------------------

/// A resolved expression where all name references have been bound.
///
/// This is a post-binding representation: every column is identified by
/// `(table_internal_id, column_index)`, every function name is a plain string,
/// and syntactic-sugar forms have been desugared.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ResolvedExpr {
    // ── Leaf nodes ─────────────────────────────────────────────────────
    /// Resolved column reference.
    Column {
        database: Option<usize>,
        table: TableInternalId,
        column: usize,
        is_rowid_alias: bool,
    },

    /// Explicit `rowid` / `oid` / `_rowid_` reference.
    RowId {
        database: Option<usize>,
        table: TableInternalId,
    },

    /// Literal value (string, numeric, blob, null, true, false).
    Literal(Literal),

    /// Bound parameter (`?`, `?NNN`, `:name`, `@name`, `$name`).
    Variable(String),

    // ── Composite nodes ────────────────────────────────────────────────
    /// Binary operation (arithmetic, comparison, logical AND/OR, concat, …).
    Binary {
        lhs: Box<ResolvedExpr>,
        op: Operator,
        rhs: Box<ResolvedExpr>,
    },

    /// Unary operation (`NOT`, `-`, `+`, `~`).
    Unary {
        op: UnaryOperator,
        expr: Box<ResolvedExpr>,
    },

    /// `COLLATE` expression.
    Collate {
        expr: Box<ResolvedExpr>,
        collation: String,
    },

    /// `CAST(expr AS type)`.
    Cast {
        expr: Box<ResolvedExpr>,
        type_name: Option<ast::Type>,
    },

    /// `IS NULL` (`negated = false`) / `IS NOT NULL` (`negated = true`).
    IsNull {
        expr: Box<ResolvedExpr>,
        negated: bool,
    },

    /// `LIKE` / `GLOB` / `MATCH` / `REGEXP`.
    Like {
        lhs: Box<ResolvedExpr>,
        negated: bool,
        op: LikeOperator,
        rhs: Box<ResolvedExpr>,
        escape: Option<Box<ResolvedExpr>>,
    },

    /// Function call with fully resolved arguments.
    FunctionCall {
        name: String,
        distinctness: Option<Distinctness>,
        args: Vec<ResolvedExpr>,
        order_by: Vec<ResolvedSortedColumn>,
        filter_clause: Option<Box<ResolvedExpr>>,
        over_clause: Option<ast::Over>,
    },

    /// `CASE` expression.
    Case {
        base: Option<Box<ResolvedExpr>>,
        when_then_pairs: Vec<(ResolvedExpr, ResolvedExpr)>,
        else_expr: Option<Box<ResolvedExpr>>,
    },

    /// `expr IN (value, …)`.
    InList {
        lhs: Box<ResolvedExpr>,
        negated: bool,
        rhs: Vec<ResolvedExpr>,
    },

    /// Planned subquery result (IN-subquery, EXISTS, scalar subquery).
    ///
    /// By the time we lower to [`ResolvedExpr`], subqueries have already been
    /// planned and their results are accessed via registers / ephemeral cursors.
    SubqueryResult {
        subquery_id: TableInternalId,
        lhs: Option<Box<ResolvedExpr>>,
        not_in: bool,
        query_type: SubqueryType,
    },

    /// `RAISE(...)` – only valid inside trigger programs.
    Raise(ResolveType, Option<Box<ResolvedExpr>>),
}

/// Resolved counterpart of [`ast::SortedColumn`].
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct ResolvedSortedColumn {
    pub expr: ResolvedExpr,
    pub order: Option<ast::SortOrder>,
    pub nulls: Option<ast::NullsOrder>,
}

// ---------------------------------------------------------------------------
// Lowering: ast::Expr (post-bind) → ResolvedExpr
// ---------------------------------------------------------------------------

/// Lower a **bound** `ast::Expr` into a [`ResolvedExpr`].
///
/// # Preconditions
///
/// The expression **must** have already been through `bind_and_rewrite_expr`
/// (or equivalent). Any remaining unresolved identifiers will cause an error.
///
/// # Panics / Errors
///
/// Returns an error if the expression contains variants that should have been
/// eliminated during binding (e.g. `Expr::Id`, `Expr::Qualified`,
/// `Expr::Between`, `Expr::Exists`, `Expr::Subquery`, `Expr::InSelect`).
#[allow(dead_code)]
pub fn lower_expr(expr: &Expr) -> Result<ResolvedExpr> {
    match expr {
        // -- resolved leaves -------------------------------------------
        Expr::Column {
            database,
            table,
            column,
            is_rowid_alias,
        } => Ok(ResolvedExpr::Column {
            database: *database,
            table: *table,
            column: *column,
            is_rowid_alias: *is_rowid_alias,
        }),

        Expr::RowId { database, table } => Ok(ResolvedExpr::RowId {
            database: *database,
            table: *table,
        }),

        Expr::Literal(lit) => Ok(ResolvedExpr::Literal(lit.clone())),

        Expr::Variable(v) => Ok(ResolvedExpr::Variable(v.clone())),

        // -- composite -------------------------------------------------
        Expr::Binary(lhs, op, rhs) => Ok(ResolvedExpr::Binary {
            lhs: Box::new(lower_expr(lhs)?),
            op: *op,
            rhs: Box::new(lower_expr(rhs)?),
        }),

        Expr::Unary(op, inner) => Ok(ResolvedExpr::Unary {
            op: *op,
            expr: Box::new(lower_expr(inner)?),
        }),

        Expr::Collate(inner, name) => Ok(ResolvedExpr::Collate {
            expr: Box::new(lower_expr(inner)?),
            collation: name.as_str().to_owned(),
        }),

        Expr::Cast { expr, type_name } => Ok(ResolvedExpr::Cast {
            expr: Box::new(lower_expr(expr)?),
            type_name: type_name.clone(),
        }),

        Expr::IsNull(inner) => Ok(ResolvedExpr::IsNull {
            expr: Box::new(lower_expr(inner)?),
            negated: false,
        }),

        Expr::NotNull(inner) => Ok(ResolvedExpr::IsNull {
            expr: Box::new(lower_expr(inner)?),
            negated: true,
        }),

        Expr::Like {
            lhs,
            not,
            op,
            rhs,
            escape,
        } => Ok(ResolvedExpr::Like {
            lhs: Box::new(lower_expr(lhs)?),
            negated: *not,
            op: *op,
            rhs: Box::new(lower_expr(rhs)?),
            escape: escape
                .as_ref()
                .map(|e| lower_expr(e).map(Box::new))
                .transpose()?,
        }),

        Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            filter_over,
        } => {
            let resolved_args = args
                .iter()
                .map(|a| lower_expr(a))
                .collect::<Result<Vec<_>>>()?;
            let resolved_order_by = order_by
                .iter()
                .map(|sc| {
                    Ok(ResolvedSortedColumn {
                        expr: lower_expr(&sc.expr)?,
                        order: sc.order,
                        nulls: sc.nulls,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ResolvedExpr::FunctionCall {
                name: name.as_str().to_owned(),
                distinctness: *distinctness,
                args: resolved_args,
                order_by: resolved_order_by,
                filter_clause: filter_over
                    .filter_clause
                    .as_ref()
                    .map(|e| lower_expr(e).map(Box::new))
                    .transpose()?,
                over_clause: filter_over.over_clause.clone(),
            })
        }

        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            let resolved_pairs = when_then_pairs
                .iter()
                .map(|(w, t)| Ok((lower_expr(w)?, lower_expr(t)?)))
                .collect::<Result<Vec<_>>>()?;
            Ok(ResolvedExpr::Case {
                base: base
                    .as_ref()
                    .map(|e| lower_expr(e).map(Box::new))
                    .transpose()?,
                when_then_pairs: resolved_pairs,
                else_expr: else_expr
                    .as_ref()
                    .map(|e| lower_expr(e).map(Box::new))
                    .transpose()?,
            })
        }

        Expr::InList { lhs, not, rhs } => {
            let resolved_rhs = rhs
                .iter()
                .map(|e| lower_expr(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(ResolvedExpr::InList {
                lhs: Box::new(lower_expr(lhs)?),
                negated: *not,
                rhs: resolved_rhs,
            })
        }

        Expr::SubqueryResult {
            subquery_id,
            lhs,
            not_in,
            query_type,
        } => Ok(ResolvedExpr::SubqueryResult {
            subquery_id: *subquery_id,
            lhs: lhs
                .as_ref()
                .map(|e| lower_expr(e).map(Box::new))
                .transpose()?,
            not_in: *not_in,
            query_type: query_type.clone(),
        }),

        Expr::Raise(resolve_type, msg) => Ok(ResolvedExpr::Raise(
            *resolve_type,
            msg.as_ref()
                .map(|e| lower_expr(e).map(Box::new))
                .transpose()?,
        )),

        // -- Parenthesized: unwrap single-element, error on row-values --
        Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                lower_expr(&exprs[0])
            } else {
                crate::bail_parse_error!(
                    "row-value Parenthesized expression with {} elements cannot be lowered to a single ResolvedExpr",
                    exprs.len()
                )
            }
        }

        // -- Register (internal for DBSP) ------------------------------
        Expr::Register(reg) => {
            // Registers are an internal mechanism. For now, preserve them
            // as a literal sentinel so that callers that still use the AST
            // path are not broken. This will go away once the full pipeline
            // operates on ResolvedExpr.
            crate::bail_parse_error!(
                "Expr::Register({reg}) encountered during lowering – this should not appear in the standard SQL compilation path"
            )
        }

        // -- Variants that must NOT exist post-binding -----------------
        Expr::Id(name) => {
            crate::bail_parse_error!(
                "unresolved identifier `{}` during lowering – bind_and_rewrite_expr was not run",
                name.as_str()
            )
        }

        Expr::Qualified(tbl, col) => {
            crate::bail_parse_error!(
                "unresolved qualified reference `{}.{}` during lowering – bind_and_rewrite_expr was not run",
                tbl.as_str(),
                col.as_str()
            )
        }

        Expr::DoublyQualified(db, tbl, col) => {
            crate::bail_parse_error!(
                "unresolved doubly-qualified reference `{}.{}.{}` during lowering – bind_and_rewrite_expr was not run",
                db.as_str(),
                tbl.as_str(),
                col.as_str()
            )
        }

        Expr::Name(name) => {
            crate::bail_parse_error!("unresolved Name `{}` during lowering", name.as_str())
        }

        Expr::Between { .. } => {
            crate::bail_parse_error!("BETWEEN should have been rewritten to AND/OR before lowering")
        }

        Expr::Exists(_) => {
            crate::bail_parse_error!(
                "EXISTS should have been planned to SubqueryResult before lowering"
            )
        }

        Expr::Subquery(_) => {
            crate::bail_parse_error!(
                "Subquery should have been planned to SubqueryResult before lowering"
            )
        }

        Expr::InSelect { .. } => {
            crate::bail_parse_error!(
                "IN (SELECT ...) should have been planned to SubqueryResult before lowering"
            )
        }

        Expr::InTable { .. } => {
            crate::bail_parse_error!("IN table-function should have been planned before lowering")
        }

        Expr::FunctionCallStar { name, .. } => {
            crate::bail_parse_error!(
                "FunctionCallStar `{}(*)` should have been expanded before lowering",
                name.as_str()
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use turso_parser::ast::{Literal, Name, Operator, UnaryOperator};

    fn make_column(table: usize, column: usize) -> Expr {
        Expr::Column {
            database: None,
            table: TableInternalId::from(table),
            column,
            is_rowid_alias: false,
        }
    }

    #[test]
    fn lower_column() {
        let ast = make_column(1, 2);
        let resolved = lower_expr(&ast).unwrap();
        assert_eq!(
            resolved,
            ResolvedExpr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 2,
                is_rowid_alias: false,
            }
        );
    }

    #[test]
    fn lower_literal() {
        let ast = Expr::Literal(Literal::Null);
        let resolved = lower_expr(&ast).unwrap();
        assert_eq!(resolved, ResolvedExpr::Literal(Literal::Null));
    }

    #[test]
    fn lower_binary() {
        let ast = Expr::Binary(
            Box::new(make_column(1, 0)),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("42".into()))),
        );
        let resolved = lower_expr(&ast).unwrap();
        match resolved {
            ResolvedExpr::Binary { lhs, op, rhs } => {
                assert_eq!(op, Operator::Equals);
                assert!(matches!(*lhs, ResolvedExpr::Column { column: 0, .. }));
                assert!(matches!(*rhs, ResolvedExpr::Literal(Literal::Numeric(_))));
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn lower_unary_not() {
        let ast = Expr::Unary(UnaryOperator::Not, Box::new(Expr::Literal(Literal::True)));
        let resolved = lower_expr(&ast).unwrap();
        match resolved {
            ResolvedExpr::Unary {
                op: UnaryOperator::Not,
                expr,
            } => {
                assert_eq!(*expr, ResolvedExpr::Literal(Literal::True));
            }
            other => panic!("expected Unary(Not, ..), got {other:?}"),
        }
    }

    #[test]
    fn lower_is_null_and_not_null() {
        let col = make_column(1, 0);

        let is_null = Expr::IsNull(Box::new(col.clone()));
        let resolved = lower_expr(&is_null).unwrap();
        assert!(matches!(
            resolved,
            ResolvedExpr::IsNull { negated: false, .. }
        ));

        let not_null = Expr::NotNull(Box::new(col));
        let resolved = lower_expr(&not_null).unwrap();
        assert!(matches!(
            resolved,
            ResolvedExpr::IsNull { negated: true, .. }
        ));
    }

    #[test]
    fn lower_case_expr() {
        let ast = Expr::Case {
            base: Some(Box::new(make_column(1, 0))),
            when_then_pairs: vec![(
                Box::new(Expr::Literal(Literal::Numeric("1".into()))),
                Box::new(Expr::Literal(Literal::String("'one'".into()))),
            )],
            else_expr: Some(Box::new(Expr::Literal(Literal::String("'other'".into())))),
        };
        let resolved = lower_expr(&ast).unwrap();
        match resolved {
            ResolvedExpr::Case {
                base: Some(_),
                when_then_pairs,
                else_expr: Some(_),
            } => {
                assert_eq!(when_then_pairs.len(), 1);
            }
            other => panic!("expected Case, got {other:?}"),
        }
    }

    #[test]
    fn lower_in_list() {
        let ast = Expr::InList {
            lhs: Box::new(make_column(1, 0)),
            not: false,
            rhs: vec![
                Box::new(Expr::Literal(Literal::Numeric("1".into()))),
                Box::new(Expr::Literal(Literal::Numeric("2".into()))),
            ],
        };
        let resolved = lower_expr(&ast).unwrap();
        match resolved {
            ResolvedExpr::InList {
                negated: false,
                rhs,
                ..
            } => {
                assert_eq!(rhs.len(), 2);
            }
            other => panic!("expected InList, got {other:?}"),
        }
    }

    #[test]
    fn lower_parenthesized_single() {
        let inner = Expr::Literal(Literal::Numeric("7".into()));
        let ast = Expr::Parenthesized(vec![Box::new(inner)]);
        let resolved = lower_expr(&ast).unwrap();
        assert_eq!(
            resolved,
            ResolvedExpr::Literal(Literal::Numeric("7".into()))
        );
    }

    #[test]
    fn lower_parenthesized_row_value_errors() {
        let ast = Expr::Parenthesized(vec![
            Box::new(Expr::Literal(Literal::Numeric("1".into()))),
            Box::new(Expr::Literal(Literal::Numeric("2".into()))),
        ]);
        assert!(lower_expr(&ast).is_err());
    }

    #[test]
    fn lower_unresolved_id_errors() {
        let ast = Expr::Id(Name::from_string("unresolved_col"));
        assert!(lower_expr(&ast).is_err());
    }

    #[test]
    fn lower_between_errors() {
        let ast = Expr::Between {
            lhs: Box::new(make_column(1, 0)),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("1".into()))),
            end: Box::new(Expr::Literal(Literal::Numeric("10".into()))),
        };
        assert!(lower_expr(&ast).is_err());
    }

    #[test]
    fn lower_variable() {
        let ast = Expr::Variable("?1".into());
        let resolved = lower_expr(&ast).unwrap();
        assert_eq!(resolved, ResolvedExpr::Variable("?1".into()));
    }

    #[test]
    fn lower_cast() {
        let ast = Expr::Cast {
            expr: Box::new(make_column(1, 0)),
            type_name: None,
        };
        let resolved = lower_expr(&ast).unwrap();
        assert!(matches!(
            resolved,
            ResolvedExpr::Cast {
                type_name: None,
                ..
            }
        ));
    }

    #[test]
    fn lower_nested_binary() {
        // (col1 = 1) AND (col2 > 5)
        let ast = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(make_column(1, 0)),
                Operator::Equals,
                Box::new(Expr::Literal(Literal::Numeric("1".into()))),
            )),
            Operator::And,
            Box::new(Expr::Binary(
                Box::new(make_column(1, 1)),
                Operator::Greater,
                Box::new(Expr::Literal(Literal::Numeric("5".into()))),
            )),
        );
        let resolved = lower_expr(&ast).unwrap();
        match resolved {
            ResolvedExpr::Binary {
                op: Operator::And,
                lhs,
                rhs,
            } => {
                assert!(matches!(
                    *lhs,
                    ResolvedExpr::Binary {
                        op: Operator::Equals,
                        ..
                    }
                ));
                assert!(matches!(
                    *rhs,
                    ResolvedExpr::Binary {
                        op: Operator::Greater,
                        ..
                    }
                ));
            }
            other => panic!("expected Binary(And, ..), got {other:?}"),
        }
    }

    #[test]
    fn lower_collate() {
        let ast = Expr::Collate(Box::new(make_column(1, 0)), Name::from_string("NOCASE"));
        let resolved = lower_expr(&ast).unwrap();
        match resolved {
            ResolvedExpr::Collate { collation, .. } => {
                assert_eq!(collation, "NOCASE");
            }
            other => panic!("expected Collate, got {other:?}"),
        }
    }

    #[test]
    fn lower_function_call() {
        let ast = Expr::FunctionCall {
            name: Name::from_string("length"),
            distinctness: None,
            args: vec![Box::new(make_column(1, 0))],
            order_by: vec![],
            filter_over: ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        let resolved = lower_expr(&ast).unwrap();
        match resolved {
            ResolvedExpr::FunctionCall { name, args, .. } => {
                assert_eq!(name, "length");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn lower_rowid() {
        let ast = Expr::RowId {
            database: None,
            table: TableInternalId::from(1),
        };
        let resolved = lower_expr(&ast).unwrap();
        assert_eq!(
            resolved,
            ResolvedExpr::RowId {
                database: None,
                table: TableInternalId::from(1),
            }
        );
    }
}
