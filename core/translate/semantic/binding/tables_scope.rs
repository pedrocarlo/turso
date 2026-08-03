// ── IdGenerator ─────────────────────────────────────────────────────────

pub trait IdGenerator {
    fn next_table_id(&mut self) -> TableInternalId;
    fn next_cte_id(&mut self) -> usize;
}

impl IdGenerator for ProgramBuilder {
    fn next_table_id(&mut self) -> ast::TableInternalId {
        self.table_reference_counter.next()
    }

    fn next_cte_id(&mut self) -> usize {
        self.alloc_cte_id()
    }
}

// ── BindTable ───────────────────────────────────────────────────────────

/// Trait for table metadata needed during binding (column name resolution).
pub trait BindTable {
    fn column_count(&self) -> usize;
    fn column_name(&self, idx: usize) -> Option<&str>;
    fn column_is_rowid_alias(&self, idx: usize) -> bool;
    fn column_is_hidden(&self, idx: usize) -> bool;
}

/// Validate the identifier leaves of an expression that binds against no
/// tables at all, such as single-row INSERT VALUES and a recursive CTE's
/// body-level LIMIT. DQS does not apply to these expressions.
fn bind_scopeless_expr(expr: &mut ast::Expr, resolver: &Resolver) -> Result<()> {
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        match expr {
            ast::Expr::Id(id) => {
                crate::bail_parse_error!("no such column: {}", id.as_str());
            }
            ast::Expr::Qualified(tbl, id) => {
                crate::bail_parse_error!("no such column: {}.{}", tbl.as_str(), id.as_str());
            }
            ast::Expr::DoublyQualified(db, tbl, id) => {
                crate::bail_parse_error!(
                    "no such column: {}.{}.{}",
                    db.as_str(),
                    tbl.as_str(),
                    id.as_str()
                );
            }
            ast::Expr::FunctionCall { name, args, .. } => {
                super::expr::validate_custom_type_function_call(name.as_str(), args, resolver)?;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

impl dyn BindTable {
    /// Create a column iterator for any `dyn BindTable`.
    pub fn columns(&self) -> BindColumnIter<'_, Self> {
        BindColumnIter {
            table: self,
            idx: 0,
        }
    }
}

pub struct BindColumnIter<'a, T: BindTable + ?Sized> {
    table: &'a T,
    idx: usize,
}

pub struct BindColumnRef<'a> {
    pub idx: usize,
    pub name: &'a str,
    pub is_rowid_alias: bool,
    pub is_hidden: bool,
}

impl<'a, T: BindTable + ?Sized> Iterator for BindColumnIter<'a, T> {
    type Item = BindColumnRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // Unnamed columns (e.g. unaliased expressions in an ephemeral scratch
        // table) cannot be referenced by name and are skipped.
        while self.idx < self.table.column_count() {
            let i = self.idx;
            self.idx += 1;
            if let Some(name) = self.table.column_name(i) {
                return Some(BindColumnRef {
                    idx: i,
                    name,
                    is_rowid_alias: self.table.column_is_rowid_alias(i),
                    is_hidden: self.table.column_is_hidden(i),
                });
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.table.column_count() - self.idx))
    }
}

impl BindTable for Table {
    fn column_count(&self) -> usize {
        self.columns().len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns().get(idx).and_then(|c| c.name.as_deref())
    }

    fn column_is_rowid_alias(&self, idx: usize) -> bool {
        self.columns().get(idx).is_some_and(|c| c.is_rowid_alias())
    }

    fn column_is_hidden(&self, idx: usize) -> bool {
        self.columns().get(idx).is_some_and(|c| c.hidden())
    }
}

/// Lightweight table for CTEs — just column names, no schema object.
pub struct CteTable {
    pub columns: Vec<String>,
}

impl BindTable for CteTable {
    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| s.as_str())
    }

    fn column_is_rowid_alias(&self, _idx: usize) -> bool {
        false
    }

    fn column_is_hidden(&self, _idx: usize) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct DerivedTable {
    pub columns: Vec<String>,
}

impl BindTable for DerivedTable {
    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| s.as_str())
    }

    fn column_is_rowid_alias(&self, _idx: usize) -> bool {
        false
    }

    fn column_is_hidden(&self, _idx: usize) -> bool {
        false
    }
}

// ── BindPhase ────────────────────────────────────────────────────────────

/// Controls alias visibility per SQL clause.
///
/// Replaces `BindingBehavior`. The phase is set on the [`BindContext`]
/// before binding each clause rather than passed per-call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindPhase {
    /// Phases 1–4: CTE, FROM, Window definitions, SELECT expressions.
    /// Only table columns visible; aliases not accessible.
    NoAliases,
    /// Phase 5: WHERE clause.
    /// Table columns first; SELECT aliases as fallback.
    TableFirst,
    /// Phases 6–8: GROUP BY, HAVING, ORDER BY.
    /// SELECT aliases first; table columns as fallback.
    AliasFirst,
}

// ── ScopeTable ───────────────────────────────────────────────────────────

/// A table visible within a single query scope.
///
/// Cheap to clone (table metadata is Arc-wrapped).
#[derive(Clone)]
pub struct ScopeTable {
    /// The name used to refer to this table in the query (original name or alias).
    pub identifier: String,
    /// Opaque ID used in `Expr::Column` to reference this table.
    pub internal_id: TableInternalId,
    /// Planner-facing source data for producing `TableReferences`.
    pub source: ScopeTableSource,
    /// Table metadata for column resolution. Clone is an Arc bump.
    pub table: Arc<dyn BindTable>,
    /// Join constraint info (USING clause for dedup during unqualified lookup).
    pub join_info: Option<JoinInfo>,
    /// Database ID for attached database support (0 = main).
    pub database_id: usize,
    /// INDEXED BY / NOT INDEXED hint from the FROM clause (real tables only).
    pub indexed: Option<ast::Indexed>,
    /// Custom index-method patterns bound to `internal_id`.
    pub bound_index_method_patterns: Vec<super::plan::BoundIndexMethodPattern>,
    /// Schema index expressions rebound to `internal_id`.
    pub bound_index_expressions: Vec<super::plan::BoundIndexExpressions>,
}

#[derive(Clone)]
pub enum ScopeTableSource {
    Table(Arc<Table>),
    Cte { name: String },
    Derived {},
}

// ── BindScope ────────────────────────────────────────────────────────────

#[derive(Clone)]
/// Snapshot of all tables visible at one query level.
///
/// Analogous to DataFusion's `DFSchema`. Owned, Arc-wrapped for cheap
/// sharing when pushed onto the outer-scope stack.
pub struct BindScope {
    pub tables: Vec<ScopeTable>,
    /// Whether tables were swapped for a RIGHT→LEFT JOIN rewrite
    /// (affects star expansion order).
    pub right_join_swapped: bool,
}

pub type BindScopeRef = Arc<BindScope>;

impl BindScope {
    pub fn empty() -> Self {
        Self {
            tables: Vec::new(),
            right_join_swapped: false,
        }
    }

    /// Find an unqualified column by name across all tables in scope.
    ///
    /// Returns `(table_internal_id, column_index, is_rowid_alias)` or `None`.
    /// Errors on ambiguity (unless deduplicated by USING clause).
    pub fn find_column_unqualified(
        &self,
        name: &str,
    ) -> Result<Option<(TableInternalId, usize, bool)>> {
        let normalized = normalize_ident(name);
        let mut result: Option<(TableInternalId, usize, bool)> = None;

        for st in &self.tables {
            let col_idx = st
                .table
                .columns()
                .position(|col| col.name.eq_ignore_ascii_case(&normalized));

            if let Some(idx) = col_idx {
                if result.is_some() {
                    let in_using = st.join_info.as_ref().is_some_and(|ji| {
                        ji.using
                            .iter()
                            .any(|u| u.as_str().eq_ignore_ascii_case(&normalized))
                    });
                    if !in_using {
                        crate::bail_parse_error!("ambiguous column name: {}", name);
                    }
                } else {
                    result = Some((st.internal_id, idx, st.table.column_is_rowid_alias(idx)));
                }
            }
        }

        Ok(result)
    }

    /// Find a qualified column (`table.column`) in this scope.
    ///
    /// SQLite allows the same table name/alias to appear more than once in a
    /// FROM clause; every table whose identifier matches is a candidate, and
    /// a column present on more than one candidate is ambiguous (unless the
    /// duplicate is deduplicated by a USING/NATURAL join on that column).
    ///
    /// Returns `None` if no table matches the identifier (caller can try
    /// outer scopes). Errors if a table matches but the column doesn't.
    pub fn find_column_qualified(
        &self,
        table_name: &str,
        col_name: &str,
    ) -> Result<Option<(TableInternalId, usize, bool)>> {
        let normalized_table = normalize_ident(table_name);
        let normalized_col = normalize_ident(col_name);

        let mut identifier_matched = false;
        let mut result: Option<(TableInternalId, usize, bool)> = None;
        for st in self
            .tables
            .iter()
            .filter(|t| t.identifier == normalized_table)
        {
            identifier_matched = true;
            let Some(idx) = st
                .table
                .columns()
                .position(|col| col.name.eq_ignore_ascii_case(&normalized_col))
            else {
                continue;
            };
            if result.is_some() {
                let in_using = st.join_info.as_ref().is_some_and(|ji| {
                    ji.using
                        .iter()
                        .any(|u| u.as_str().eq_ignore_ascii_case(&normalized_col))
                });
                if !in_using {
                    crate::bail_parse_error!("ambiguous column name: {}.{}", table_name, col_name);
                }
                continue;
            }
            result = Some((st.internal_id, idx, st.table.column_is_rowid_alias(idx)));
        }

        if !identifier_matched {
            return Ok(None);
        }
        let Some(found) = result else {
            crate::bail_parse_error!("no such column: {}.{}", table_name, col_name);
        };
        Ok(Some(found))
    }

    /// Find a table by its identifier (name or alias).
    pub fn find_table_by_identifier(&self, name: &str) -> Option<&ScopeTable> {
        let normalized = normalize_ident(name);
        self.tables.iter().find(|t| t.identifier == normalized)
    }
}

