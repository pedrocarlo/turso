impl<'a, G: IdGenerator> BindContext<'a, G> {
    pub fn new(resolver: &'a Resolver<'a>, id_gen: &'a mut G) -> Self {
        Self {
            resolver,
            id_gen,
            outer_query_frames: Vec::new(),
            outer_frame_floor: 0,
            outer_from_scope: None,
            ctes: HashMap::default(),
            ctes_being_bound: Vec::new(),
            aliases: Arc::new(Vec::new()),
            phase: BindPhase::NoAliases,
            allow_unbound: false,
            tracking: BindTracking::default(),
            subquery_bindings: HashMap::default(),
            shared_subqueries: Vec::new(),
            derived_bindings: HashMap::default(),
            recursive_self: None,
            trigger_columns: None,
        }
    }

    // ── Outer scope stack (mirrors DataFusion PlannerContext) ─────────

    /// Push a scope onto the outer-scope stack (entering a subquery).
    fn append_outer_query_scope(&mut self, scope: BindScopeRef, aliases: Arc<Vec<BoundColumn>>) {
        self.outer_query_frames
            .push(OuterQueryFrame { scope, aliases });
    }

    /// Pop the most recent outer scope (exiting a subquery).
    fn pop_outer_query_scope(&mut self) -> Option<OuterQueryFrame> {
        self.outer_query_frames.pop()
    }

    /// Iterate outer scopes innermost-first (reversed storage order).
    /// Matches column lookup precedence: nearest enclosing query first.
    /// Frames below `outer_frame_floor` are hidden (see the field docs).
    fn outer_scopes_iter(&self) -> impl Iterator<Item = &BindScopeRef> {
        self.outer_query_frames[self.outer_frame_floor..]
            .iter()
            .rev()
            .map(|frame| &frame.scope)
    }

    fn outer_query_frames_iter(&self) -> impl Iterator<Item = &OuterQueryFrame> {
        self.outer_query_frames[self.outer_frame_floor..]
            .iter()
            .rev()
    }

    /// Iterate ALL outer scopes, including frames hidden by
    /// `outer_frame_floor`. Only for error reporting (naming a table that
    /// exists but is not referenceable from the current clause).
    fn all_outer_scopes_iter(&self) -> impl Iterator<Item = &BindScopeRef> {
        self.outer_query_frames
            .iter()
            .rev()
            .map(|frame| &frame.scope)
    }

    // ── CTEs ─────────────────────────────────────────────────────────

    pub fn insert_cte(&mut self, name: String, entry: CteEntry) {
        self.ctes.insert(name, entry);
    }

    #[cfg(test)]
    fn get_cte(&self, name: &str) -> Option<&CteEntry> {
        self.ctes.get(name)
    }

    // ── Phase and aliases ────────────────────────────────────────────

    fn phase(&self) -> BindPhase {
        self.phase
    }

    fn set_aliases(&mut self, aliases: Arc<Vec<BoundColumn>>) {
        self.aliases = aliases;
    }

    fn aliases(&self) -> &[BoundColumn] {
        &self.aliases
    }

    /// Run `f` with a fresh per-select-core state (phase, aliases).
    /// Saves and restores on exit so individual SELECT cores in the same
    /// compound query do not clobber each other.
    fn with_scope<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let saved_aliases = std::mem::take(&mut self.aliases);
        let saved_phase = self.phase;
        let saved_shared = std::mem::take(&mut self.shared_subqueries);

        let result = f(self);

        self.aliases = saved_aliases;
        self.phase = saved_phase;
        self.shared_subqueries = saved_shared;

        result
    }

    /// Run `f` with a fresh query state, restoring CTE/alias/phase state on exit.
    ///
    /// This mirrors DataFusion's per-query PlannerContext cloning semantics:
    /// subqueries inherit outer CTEs, but their own WITH items remain private.
    fn with_query<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        // Swap out current CTEs (preserving inner_bound) and give the inner
        // query a clone (inner_bound = None is fine — inner queries don't plan
        // outer CTEs, they only need names/columns for resolution).
        let mut saved_ctes = self.ctes.clone(); // clone for inner query
        std::mem::swap(&mut self.ctes, &mut saved_ctes); // saved_ctes now has originals
        let saved_aliases = std::mem::take(&mut self.aliases);
        let saved_phase = self.phase;
        let saved_outer_from_scope = self.outer_from_scope.clone();
        let saved_tracking = std::mem::take(&mut self.tracking);
        let saved_floor = self.outer_frame_floor;
        let saved_subquery_bindings = std::mem::take(&mut self.subquery_bindings);
        let saved_derived_bindings = std::mem::take(&mut self.derived_bindings);

        let result = f(self);

        self.ctes = saved_ctes;
        self.aliases = saved_aliases;
        self.phase = saved_phase;
        self.outer_from_scope = saved_outer_from_scope;
        self.tracking = saved_tracking;
        self.outer_frame_floor = saved_floor;
        self.subquery_bindings = saved_subquery_bindings;
        self.derived_bindings = saved_derived_bindings;

        result
    }

    /// Run `f` with a temporary phase, restoring the previous phase on exit.
    fn with_phase<T>(
        &mut self,
        phase: BindPhase,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let saved = self.phase;
        self.phase = phase;
        let result = f(self);
        self.phase = saved;
        result
    }

    /// Extract result columns from a SELECT list before the main bind pass.
    ///
    /// Captures the name and a bound expression for each result column.
    /// For identifiers and star expansions, the expression is resolved
    /// to `Expr::Column` immediately. For complex expressions, the
    /// original AST is cloned and bound via `bind_expr`.
    ///
    /// Must be called before `bind_select_list` rewrites the AST in-place,
    /// since we need the raw identifiers to infer column names.
