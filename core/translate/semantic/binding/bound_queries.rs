// ── BoundColumn ─────────────────────────────────────────────────────────

/// A resolved result column from a SELECT list.
/// Used for alias resolution in later phases (WHERE, GROUP BY, ORDER BY)
/// and for propagating column names to CTEs/subqueries.
#[derive(Clone)]
pub struct BoundColumn {
    /// The column name — explicit alias or inferred from the expression.
    pub name: String,
    /// The original expression (before binding), cloned into alias references.
    pub expr: ast::Expr,
    /// True if the name comes from an explicit AS alias (not inferred from expr).
    pub is_explicit_alias: bool,
}

/// A subquery expression that was bound during binding.
/// The inner `ast::Select` is already bound (column refs resolved).
/// The planner uses this to plan the subquery without re-binding.
pub struct BoundSubquery {
    /// The bound inner SELECT.
    pub select: ast::Select,
    /// Inner binding results (scopes → table references).
    pub inner_bound: BoundSelect,
}

/// Bound arms of a recursive CTE body, produced at bind time and consumed by
/// the recursive-CTE planner.
pub struct RecursiveCteBinding {
    /// The initial (non-recursive) arms as one bound SELECT, including the
    /// body-level WITH clause. SQLite takes the CTE's column names and arity
    /// from the left-most arm, which cannot reference the recursive table.
    pub initial: BoundSubquery,
    /// Each recursive arm bound as its own single-arm SELECT. The body-level
    /// WITH clause is cloned into each arm, matching the per-arm nested CTE
    /// planning of compound SELECTs on the raw path.
    pub recursive_arms: Vec<BoundSubquery>,
    /// Index of the first recursive arm (arm 0 is the body's first SELECT),
    /// as returned by `validate_recursive_cte_structure`.
    pub first_recursive_arm_index: usize,
    /// The table id every self-reference in the recursive arms was bound to.
    /// All recursive arms read the same recursive input table, so the planner
    /// creates that table with this id.
    pub input_id: ast::TableInternalId,
    /// Body-level ORDER BY resolved to output-column positions.
    pub queue_order: Option<Vec<super::plan::CompoundOrderByKey>>,
}

/// Set while a recursive CTE's recursive arms are being bound: the CTE's own
/// name resolves to the recursive input table instead of raising a circular
/// reference.
struct RecursiveSelfRef {
    /// Identity of the CTE being bound (a shadowing nested CTE with the same
    /// name has a different id and resolves normally).
    cte_id: usize,
    /// Shared id for every self-reference (see [RecursiveCteBinding::input_id]).
    input_id: ast::TableInternalId,
    /// Column metadata for resolving references to the recursive table.
    table: Arc<CteTable>,
}

pub struct BoundSelect {
    pub result_columns: Arc<Vec<BoundColumn>>,
    /// Result-column metadata for each compound arm after the first.
    pub compound_result_columns: Vec<Arc<Vec<BoundColumn>>>,
    /// Compound ORDER BY resolved to output-column positions.
    pub compound_order_by: Option<Vec<super::plan::CompoundOrderByKey>>,
    pub main_scope: BindScope,
    pub compound_scopes: Vec<BindScope>,
    pub tracking: BindTracking,
    /// Expression subqueries (EXISTS, scalar subquery, IN SELECT) keyed by
    /// the `subquery_id` stored in the corresponding `Expr::SubqueryResult`.
    pub subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    /// CTE definitions from the WITH clause, in definition order.
    /// Populated only for the top-level select that owns the WITH clause.
    pub cte_definitions: Vec<(String, CteEntry)>,
    /// FROM-clause subqueries (derived tables), keyed by the scope table's
    /// `internal_id`. Planned before `into_table_references` and looked up
    /// by the `Derived` arm in `scope_to_table_references`.
    pub derived_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
}

fn ordinal(n: usize) -> String {
    let suffix = match (n % 10, n % 100) {
        (1, 11) | (2, 12) | (3, 13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{n}{suffix}")
}

fn resolve_compound_order_by_expr(
    expr: &ast::Expr,
    result_column_arms: &[&[BoundColumn]],
    term_number: usize,
) -> Result<(usize, Option<super::collate::CollationSeq>)> {
    let num_result_columns = result_column_arms
        .first()
        .expect("compound SELECT must have a first arm")
        .len();
    match expr {
        ast::Expr::Collate(inner, collation_name) => {
            let (column, _) =
                resolve_compound_order_by_expr(inner, result_column_arms, term_number)?;
            Ok((
                column,
                Some(super::collate::CollationSeq::new(collation_name.as_str())?),
            ))
        }
        ast::Expr::Literal(ast::Literal::Numeric(number)) => {
            let Ok(column_number) = number.parse::<i32>() else {
                crate::bail_parse_error!(
                    "{} ORDER BY term does not match any column in the result set",
                    ordinal(term_number)
                );
            };
            if column_number <= 0 || column_number as usize > num_result_columns {
                crate::bail_parse_error!(
                    "{} ORDER BY term out of range - should be between 1 and {}",
                    column_number,
                    num_result_columns
                );
            }
            Ok((column_number as usize - 1, None))
        }
        ast::Expr::Id(name) => {
            let normalized = normalize_ident(name.as_str());
            for result_columns in result_column_arms {
                if let Some((column, _)) = result_columns.iter().enumerate().find(|(_, column)| {
                    column.is_explicit_alias && column.name.eq_ignore_ascii_case(&normalized)
                }) {
                    return Ok((column, None));
                }
                if let Some((column, _)) = result_columns.iter().enumerate().find(|(_, column)| {
                    !column.name.is_empty() && column.name.eq_ignore_ascii_case(&normalized)
                }) {
                    return Ok((column, None));
                }
            }
            crate::bail_parse_error!(
                "{} ORDER BY term does not match any column in the result set",
                ordinal(term_number)
            );
        }
        _ => crate::bail_parse_error!(
            "{} ORDER BY term does not match any column in the result set",
            ordinal(term_number)
        ),
    }
}

fn resolve_compound_order_by(
    order_by: &[ast::SortedColumn],
    result_column_arms: &[&[BoundColumn]],
) -> Result<Option<Vec<super::plan::CompoundOrderByKey>>> {
    if order_by.is_empty() {
        return Ok(None);
    }
    order_by
        .iter()
        .enumerate()
        .map(|(index, term)| {
            let (column, collation) =
                resolve_compound_order_by_expr(&term.expr, result_column_arms, index + 1)?;
            Ok((
                column,
                term.order.unwrap_or(ast::SortOrder::Asc),
                term.nulls,
                collation,
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

pub struct BoundUpdate {
    /// Scope containing only the target table (with alias/INDEXED BY).
    pub target_scope: BindScope,
    /// Scope for the UPDATE ... FROM clause tables, if present.
    pub from_scope: Option<BindScope>,
    pub tracking: BindTracking,
    pub subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    pub derived_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    pub cte_definitions: Vec<(String, CteEntry)>,
    /// Database the target table lives in (0 = main).
    pub database_id: usize,
    /// The validated target table.
    pub table: Arc<Table>,
    /// `OR <conflict>` clause taken off the statement during binding.
    pub or_conflict: Option<ast::ResolveType>,
}

impl BoundUpdate {
    /// Convert the target scope into a single-table `TableReferences`.
    pub fn target_table_references(
        &self,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
    ) -> Result<TableReferences> {
        BoundSelect::scope_to_table_references(
            self.target_scope.clone(),
            &self.tracking,
            planned_ctes,
            &mut HashMap::default(),
            Vec::new(),
        )
    }

    /// Convert the FROM-clause scope (if any) into `TableReferences`.
    #[allow(clippy::wrong_self_convention)]
    pub fn from_table_references(
        &mut self,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
        planned_derived: &mut HashMap<ast::TableInternalId, super::plan::JoinedTable>,
    ) -> Result<TableReferences> {
        let Some(scope) = self.from_scope.take() else {
            return Ok(TableReferences::new_empty());
        };
        BoundSelect::scope_to_table_references(
            scope,
            &self.tracking,
            planned_ctes,
            planned_derived,
            Vec::new(),
        )
    }
}

pub struct BoundDelete {
    pub scope: BindScope,
    pub tracking: BindTracking,
    pub subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    pub cte_definitions: Vec<(String, CteEntry)>,
    /// Database the target table lives in (0 = main).
    pub database_id: usize,
    /// The validated target table.
    pub table: Arc<Table>,
}

impl BoundDelete {
    pub fn into_table_references(
        self,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
    ) -> Result<TableReferences> {
        BoundSelect::scope_to_table_references(
            self.scope,
            &self.tracking,
            planned_ctes,
            &mut HashMap::default(),
            Vec::new(),
        )
    }
}

