// ── BindContext ───────────────────────────────────────────────────────────

/// Scope-aware binding context, analogous to DataFusion's `PlannerContext`.
///
/// Manages the outer-scope stack for correlated subquery resolution,
/// CTE definitions, SELECT aliases, and binding phase tracking.
///
/// Does **not** borrow `TableReferences`. Column usage is recorded in
/// [`BindTracking`] and flushed back after binding completes.
pub struct BindContext<'a, G: IdGenerator> {
    /// Function and schema resolver.
    pub resolver: &'a Resolver<'a>,

    /// Generates unique table IDs for scope tables.
    id_gen: &'a mut G,

    /// Stack of outer query scopes plus visible aliases.
    outer_query_frames: Vec<OuterQueryFrame>,

    /// Index into `outer_query_frames` below which frames are invisible to
    /// column resolution. GROUP BY expressions cannot reference the outer
    /// query scope (SQLite rejects them with "no such column"), but the
    /// frames must stay physically present so error messages can still name
    /// the outer table (`no such column: t2.d`, not `no such table: t2`).
    outer_frame_floor: usize,

    /// Outer FROM schema for LATERAL join support.
    outer_from_scope: Option<BindScopeRef>,

    /// CTE definitions visible in the current query.
    ctes: HashMap<String, CteEntry>,

    /// `(cte_id, name)` of CTEs whose bodies are currently being bound. A
    /// reference to one of these from inside another CTE body is a circular
    /// reference. Tracked by id so a shadowing nested CTE with the same name
    /// is not mistaken for the in-flight one.
    ctes_being_bound: Vec<(usize, String)>,

    /// SELECT result columns for alias resolution in later phases.
    /// Populated after binding the SELECT list. Arc-shared so pushing a
    /// subquery frame or switching phases never deep-clones the column
    /// expressions (that cost scales with SELECT-list width).
    aliases: Arc<Vec<BoundColumn>>,

    /// Current binding phase — controls alias visibility.
    phase: BindPhase,

    /// When true, unresolved identifiers are left as-is instead of erroring.
    /// Used for UPSERT DO UPDATE SET/WHERE where `EXCLUDED.col` can't be
    /// resolved at bind time.
    allow_unbound: bool,

    /// Records column/rowid usage for post-binding flush.
    pub tracking: BindTracking,

    /// Expression subqueries bound during this query, keyed by subquery_id.
    /// Moved into `BoundSelect` when binding completes.
    subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,

    /// Scalar subqueries shared between GROUP BY and the SELECT list of the
    /// current SELECT core. Each entry holds the raw (pre-bind) expression and
    /// the subquery id assigned when the first occurrence is bound; later
    /// occurrences are rewritten to the same id so they share one evaluation
    /// (and compare equal for GROUP BY expression matching), mirroring the
    /// planner's scalar-subquery CSE.
    shared_subqueries: Vec<(ast::Expr, Option<ast::TableInternalId>)>,

    /// FROM-clause subqueries (derived tables) bound during this query,
    /// keyed by the scope table's `internal_id`.
    derived_bindings: HashMap<ast::TableInternalId, BoundSubquery>,

    /// Set while binding a recursive CTE's recursive arms: the CTE's own name
    /// resolves to the recursive input table instead of raising a circular
    /// reference.
    recursive_self: Option<RecursiveSelfRef>,

    /// NEW/OLD values visible while binding a trigger WHEN clause.
    trigger_columns: Option<TriggerColumnBindings>,
}

