// ── BindTracking ─────────────────────────────────────────────────────────

/// Records what was accessed during binding.
///
/// Applied to `TableReferences` in a single flush after binding completes.
#[derive(Debug, Default)]
pub struct BindTracking {
    /// `(table_id, column_index)` pairs for columns referenced in the current scope.
    pub columns_used: Vec<(TableInternalId, usize)>,
    /// Tables whose rowid was referenced.
    pub rowids_used: Vec<TableInternalId>,
    /// `(table_id, column_index)` pairs for columns referenced from outer scopes.
    pub outer_refs_used: Vec<(TableInternalId, usize)>,
}

impl BindTracking {
    pub fn record_column(&mut self, table_id: TableInternalId, col_idx: usize) {
        self.columns_used.push((table_id, col_idx));
    }

    pub fn record_rowid(&mut self, table_id: TableInternalId) {
        self.rowids_used.push(table_id);
    }

    pub fn record_outer_ref(&mut self, table_id: TableInternalId, col_idx: usize) {
        self.outer_refs_used.push((table_id, col_idx));
    }

    /// Apply recorded usage back to `TableReferences`.
    ///
    /// Tracking spans the whole statement (main scope, compound scopes,
    /// subquery scopes), but each `TableReferences` only holds one scope's
    /// tables — usages recorded for other scopes are skipped here and flushed
    /// when their own scope is converted.
    pub fn flush(&self, table_references: &mut TableReferences) {
        let contains = |tr: &TableReferences, id: TableInternalId| {
            tr.find_joined_table_by_internal_id(id).is_some()
                || tr.find_outer_query_ref_by_internal_id(id).is_some()
        };
        for &(table_id, col_idx) in &self.columns_used {
            if contains(table_references, table_id) {
                table_references.mark_column_used(table_id, col_idx);
            }
        }
        for &table_id in &self.rowids_used {
            table_references.mark_rowid_referenced(table_id);
        }
        for &(table_id, col_idx) in &self.outer_refs_used {
            if contains(table_references, table_id) {
                table_references.mark_column_used(table_id, col_idx);
            }
        }
    }
}

// ── CteEntry ─────────────────────────────────────────────────────────────

/// A CTE definition stored in the binding context.
///
/// `Clone` copies metadata (name, columns, IDs) but sets `inner_bound` to `None`.
/// This is intentional: `with_query` clones CTEs for subquery scoping, where only
/// the column/name info is needed for resolution, not the full binding output.
pub struct CteEntry {
    /// The bound AST (column refs resolved).
    pub select: ast::Select,
    /// Explicit column names from `WITH t(a, b) AS (...)`.
    pub explicit_columns: Vec<String>,
    /// Globally unique CTE identity for materialization tracking.
    pub cte_id: usize,
    /// Result column names, populated after binding the CTE body.
    /// If explicit_columns is non-empty, equals explicit_columns.
    /// Otherwise, extracted from the SELECT result columns.
    pub resolved_columns: Vec<String>,
    /// Number of result columns produced by the CTE body's SELECT.
    /// Used to validate explicit column names when the CTE is referenced
    /// (SQLite defers this check until an actual reference).
    pub result_column_count: usize,
    /// Inner binding results (scopes, tracking, subquery bindings).
    pub inner_bound: Option<BoundSelect>,
    /// Indexes of CTEs (in definition order) that this CTE directly references.
    pub referenced_cte_indices: SmallVec<[usize; 2]>,
    /// True if `AS MATERIALIZED` was specified, forcing materialization.
    pub materialize_hint: bool,
    /// True if the body references its own name (a recursive CTE, whether or
    /// not the RECURSIVE keyword was written). `select` keeps the raw body
    /// (the planner reads compound operators and LIMIT from it); bound arms
    /// and resolved queue ordering live in `recursive_binding`.
    pub recursive: bool,
    /// Bound arms of a recursive CTE body (`recursive` is true), consumed by
    /// the recursive-CTE planner. `None` for non-recursive entries.
    pub recursive_binding: Option<RecursiveCteBinding>,
    /// Binding error deferred until the CTE is referenced. SQLite never
    /// resolves the body of an unused CTE, so errors in unused bodies (bad
    /// columns, circular references) must not surface eagerly.
    pub bind_error: Option<String>,
}

impl Clone for CteEntry {
    fn clone(&self) -> Self {
        Self {
            select: self.select.clone(),
            explicit_columns: self.explicit_columns.clone(),
            cte_id: self.cte_id,
            resolved_columns: self.resolved_columns.clone(),
            result_column_count: self.result_column_count,
            inner_bound: None,
            referenced_cte_indices: self.referenced_cte_indices.clone(),
            materialize_hint: self.materialize_hint,
            recursive: self.recursive,
            recursive_binding: None,
            bind_error: self.bind_error.clone(),
        }
    }
}

