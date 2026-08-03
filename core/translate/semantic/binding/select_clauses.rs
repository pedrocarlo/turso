    fn extract_bound_columns(
        &mut self,
        columns: &mut [ast::ResultColumn],
        scope: &BindScope,
        expected_types: &[Option<Arc<TypeDef>>],
    ) -> Result<Vec<BoundColumn>> {
        let mut result = Vec::with_capacity(columns.len());
        let mut output_index = 0;
        for col in columns {
            match col {
                ast::ResultColumn::Expr(expr, alias) => {
                    // Determine the column name. Implicit column names (the
                    // original SQL text preserved by the parser for unaliased
                    // expressions) are only a naming fallback, not an alias.
                    let explicit_alias = alias.as_ref().filter(|a| a.is_explicit());
                    let name = if let Some(a) = explicit_alias {
                        normalize_ident(a.name().as_str())
                    } else {
                        let inferred = match expr.as_ref() {
                            ast::Expr::Id(id) => normalize_ident(id.as_str()),
                            ast::Expr::Qualified(_, id) => normalize_ident(id.as_str()),
                            ast::Expr::DoublyQualified(_, _, id) => normalize_ident(id.as_str()),
                            // After star expansion, columns are Expr::Column with
                            // table internal_id and column index. Look up the name
                            // from the scope.
                            ast::Expr::Column {
                                table: table_id,
                                column: col_idx,
                                ..
                            } => scope
                                .tables
                                .iter()
                                .find(|st| st.internal_id == *table_id)
                                .and_then(|st| st.table.column_name(*col_idx))
                                .map(|n| n.to_string())
                                .unwrap_or_default(),
                            // Complex expressions without an alias can't be
                            // referenced by name from outer queries.
                            _ => String::new(),
                        };
                        if inferred.is_empty() {
                            // Fall back to the implicit column name (original
                            // expression text), matching ResultSetColumn::name.
                            alias
                                .as_ref()
                                .map(|a| a.name().as_str().to_string())
                                .unwrap_or_default()
                        } else {
                            inferred
                        }
                    };
                    let is_explicit_alias = explicit_alias.is_some();
                    // Resolve the expression
                    self.bind_expr_with_expected_type(
                        expr,
                        scope,
                        expected_types.get(output_index).and_then(Option::as_deref),
                    )?;
                    result.push(BoundColumn {
                        name,
                        expr: expr.as_ref().clone(),
                        is_explicit_alias,
                    });
                    output_index += 1;
                }
                ast::ResultColumn::Star => {
                    // The star stays unexpanded in the AST (the planner's
                    // `select_star` produces the plan columns), but its names
                    // and arity are needed here for alias resolution and
                    // compound-select checks. Mirror `select_star`'s
                    // visibility rules exactly: ordering under RIGHT JOIN
                    // swapping, semi/anti-join exclusion, ambiguity on
                    // duplicate identifiers, hidden columns, USING dedup.
                    let table_iter: Vec<&ScopeTable> = if scope.right_join_swapped {
                        scope.tables.iter().rev().collect()
                    } else {
                        scope.tables.iter().collect()
                    };
                    for st in table_iter {
                        if st.join_info.as_ref().is_some_and(|ji| ji.is_semi_or_anti()) {
                            continue;
                        }
                        // If this table's identifier appears more than once in
                        // the FROM clause, expanding * would produce ambiguous
                        // column references (matches SQLite). Columns
                        // deduplicated by USING/NATURAL are not ambiguous.
                        let has_duplicate_identifier = scope
                            .tables
                            .iter()
                            .filter(|t| t.identifier == st.identifier)
                            .count()
                            > 1;
                        if has_duplicate_identifier {
                            let using_cols: Vec<&str> = scope
                                .tables
                                .iter()
                                .filter(|t| t.identifier == st.identifier)
                                .filter_map(|t| t.join_info.as_ref())
                                .flat_map(|ji| ji.using.iter().map(|u| u.as_str()))
                                .collect();
                            for col_ref in st.table.columns() {
                                if col_ref.is_hidden {
                                    continue;
                                }
                                let in_using = using_cols
                                    .iter()
                                    .any(|u| u.eq_ignore_ascii_case(col_ref.name));
                                if !in_using {
                                    crate::bail_parse_error!(
                                        "ambiguous column name: {}.{}",
                                        st.identifier,
                                        col_ref.name
                                    );
                                }
                            }
                        }
                        for col_ref in st.table.columns() {
                            if col_ref.is_hidden {
                                continue;
                            }
                            // USING dedup: skip right-table columns named in USING
                            if let Some(ji) = &st.join_info {
                                if ji
                                    .using
                                    .iter()
                                    .any(|u| u.as_str().eq_ignore_ascii_case(col_ref.name))
                                {
                                    continue;
                                }
                            }
                            result.push(BoundColumn {
                                name: col_ref.name.to_string(),
                                expr: ast::Expr::Column {
                                    database: None,
                                    table: st.internal_id,
                                    column: col_ref.idx,
                                    is_rowid_alias: col_ref.is_rowid_alias,
                                },
                                is_explicit_alias: false,
                            });
                            output_index += 1;
                        }
                    }
                }
                ast::ResultColumn::TableStar(table_name) => {
                    let Some(st) = scope.find_table_by_identifier(table_name.as_str()) else {
                        crate::bail_parse_error!("no such table: {}", table_name);
                    };
                    for col_ref in st.table.columns() {
                        if col_ref.is_hidden {
                            continue;
                        }
                        result.push(BoundColumn {
                            name: col_ref.name.to_string(),
                            expr: ast::Expr::Column {
                                database: None,
                                table: st.internal_id,
                                column: col_ref.idx,
                                is_rowid_alias: col_ref.is_rowid_alias,
                            },
                            is_explicit_alias: false,
                        });
                        output_index += 1;
                    }
                }
            }
        }
        Ok(result)
    }

    /// Bind a SELECT statement, resolving all name references in-place.
    /// Returns the bound query result needed by planning.
    pub fn bind_select(&mut self, select: &mut ast::Select) -> Result<BoundSelect> {
        self.bind_select_with_expected_types(select, &[])
    }

    /// Bind a SELECT used as an INSERT source. Each result expression receives
    /// the type of the destination column at the same position.
    fn bind_select_with_expected_types(
        &mut self,
        select: &mut ast::Select,
        expected_types: &[Option<Arc<TypeDef>>],
    ) -> Result<BoundSelect> {
        self.with_query(|ctx| {
            // 1. Bind CTEs from WITH clause
            if let Some(with) = &mut select.with {
                ctx.bind_cte(with)?;
            }

            // 2. Bind the main OneSelect. Its aliases and FROM scope are the ones
            // visible to the query-level ORDER BY.
            let (result_columns, main_scope) =
                ctx.bind_one_select(&mut select.body.select, expected_types)?;

            // 3. Bind compound selects (UNION, INTERSECT, EXCEPT)
            let mut compound_scopes = Vec::with_capacity(select.body.compounds.len());
            let mut compound_result_columns = Vec::with_capacity(select.body.compounds.len());
            for compound in &mut select.body.compounds {
                let (compound_columns, compound_scope) =
                    ctx.bind_one_select(&mut compound.select, expected_types)?;
                compound_result_columns.push(compound_columns);
                compound_scopes.push(compound_scope);
            }

            // 4. Bind ORDER BY (AliasFirst phase — aliases take priority).
            ctx.set_aliases(Arc::clone(&result_columns));
            let compound_order_by = if select.body.compounds.is_empty() {
                ctx.with_phase(BindPhase::AliasFirst, |ctx| {
                    for sort_col in &mut select.order_by {
                        ctx.replace_column_number(&mut sort_col.expr)?;
                        // Optimize trivial subqueries like (SELECT alias) by inlining
                        // the alias expression. This avoids creating a "correlated"
                        // subquery that's really just an alias reference.
                        ctx.try_inline_trivial_subquery(&mut sort_col.expr, &main_scope);
                        ctx.bind_expr(&mut sort_col.expr, &main_scope)?;
                    }
                    Ok(())
                })?;
                None
            } else {
                let right_most_count = compound_result_columns
                    .last()
                    .expect("compound SELECT must have a right-most arm")
                    .len();
                let mut left_counts = std::iter::once(result_columns.len())
                    .chain(
                        compound_result_columns[..compound_result_columns.len() - 1]
                            .iter()
                            .map(|columns| columns.len()),
                    )
                    .zip(select.body.compounds.iter().map(|compound| compound.operator));
                if let Some((_, operator)) =
                    left_counts.find(|(column_count, _)| *column_count != right_most_count)
                {
                    crate::bail_parse_error!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        operator
                    );
                }

                let result_column_arms: Vec<&[BoundColumn]> =
                    std::iter::once(result_columns.as_slice())
                        .chain(
                            compound_result_columns
                                .iter()
                                .map(|columns| columns.as_slice()),
                        )
                        .collect();
                let resolved =
                    resolve_compound_order_by(&select.order_by, &result_column_arms)?;
                select.order_by.clear();
                resolved
            };

            // 5. Bind LIMIT/OFFSET (no scope — these are standalone expressions)
            if let Some(limit) = select.limit.as_mut() {
                let empty = BindScope::empty();
                ctx.bind_expr(&mut limit.expr, &empty)?;
                if let Some(offset) = limit.offset.as_mut() {
                    ctx.bind_expr(offset, &empty)?;
                }
            }

            // 6. Extract CTE definitions in definition order before with_query
            //    restores them. Using definition order is critical because
            //    referenced_cte_indices are offsets into this order.
            let cte_definitions: Vec<(String, CteEntry)> = if let Some(with) = &select.with {
                let mut ctes = std::mem::take(&mut ctx.ctes);
                with.ctes
                    .iter()
                    .filter_map(|cte| {
                        let name = normalize_ident(cte.tbl_name.as_str());
                        ctes.remove(&name).map(|entry| (name, entry))
                    })
                    .collect()
            } else {
                vec![]
            };

            Ok(BoundSelect {
                result_columns,
                compound_result_columns,
                compound_order_by,
                main_scope,
                compound_scopes,
                tracking: std::mem::take(&mut ctx.tracking),
                subquery_bindings: std::mem::take(&mut ctx.subquery_bindings),
                cte_definitions,
                derived_bindings: std::mem::take(&mut ctx.derived_bindings),
            })
        })
    }

    /// Bind a single SELECT (not compound). Returns bound result columns,
    /// the scope, and the join order.
    fn bind_one_select(
        &mut self,
        one: &mut ast::OneSelect,
        expected_types: &[Option<Arc<TypeDef>>],
    ) -> Result<(Arc<Vec<BoundColumn>>, BindScope)> {
        self.with_scope(|ctx| {
            match one {
                ast::OneSelect::Select {
                    columns,
                    from,
                    where_clause,
                    group_by,
                    window_clause,
                    ..
                } => {
                    // 1. Bind FROM → build scope
                    let scope = match from {
                        Some(from) => ctx.bind_from(from)?,
                        None => {
                            // Check for Star/TableStar without FROM before expansion
                            for col in columns.iter() {
                                if matches!(col, ast::ResultColumn::Star) {
                                    crate::bail_parse_error!("no tables specified");
                                }
                            }
                            BindScope::empty()
                        }
                    };

                    // 2. Star/TableStar result columns stay unexpanded in the
                    // AST: `extract_bound_columns` derives their names and
                    // arity for alias resolution, and the planner's
                    // `select_star` fast path expands them into plan columns
                    // without materializing per-column AST nodes (which is
                    // wasteful for wide tables).

                    // 3. Bind WINDOW definitions (NoAliases — same phase as SELECT list)
                    ctx.with_phase(BindPhase::NoAliases, |ctx| {
                        ctx.bind_window_defs(window_clause, &scope)
                    })?;

                    // 5a. Detect scalar subqueries shared between GROUP BY and
                    //     the SELECT list before either is bound. Shared
                    //     occurrences get one subquery id (see bind_expr).
                    ctx.collect_shared_subqueries(columns, group_by.as_ref());

                    // 5. Extract bound columns (names + resolved exprs) before
                    //    the main bind pass rewrites the AST in-place.
                    let bound_columns =
                        Arc::new(ctx.extract_bound_columns(columns, &scope, expected_types)?);

                    // 6. Store as aliases for later phases (WHERE, GROUP BY, ORDER BY)
                    ctx.set_aliases(Arc::clone(&bound_columns));

                    // 7. Bind SELECT expressions in-place (NoAliases phase)
                    ctx.with_phase(BindPhase::NoAliases, |ctx| {
                        ctx.bind_select_list(columns, &scope)
                    })?;

                    // 8. Bind WHERE (TableFirst phase — table columns first, aliases as fallback)
                    if let Some(where_expr) = where_clause {
                        ctx.with_phase(BindPhase::TableFirst, |ctx| {
                            ctx.bind_expr(where_expr, &scope)
                        })?;
                    }

                    // 9. Bind GROUP BY and HAVING. In GROUP BY, real columns
                    //    take precedence over SELECT aliases (TableFirst);
                    //    HAVING prefers aliases (AliasFirst) — matching SQLite.
                    if let Some(group_by) = group_by {
                        ctx.with_phase(BindPhase::TableFirst, |ctx| {
                            ctx.bind_group_by(group_by, &scope)
                        })?;
                    }

                    Ok((bound_columns, scope))
                }
                ast::OneSelect::Values(rows) => {
                    let scope = BindScope::empty();
                    // Generate column1, column2, ... names from the arity
                    // of the first VALUES row (matching SQLite behavior).
                    let num_cols = rows.first().map_or(0, |row| row.len());
                    let bound_columns: Arc<Vec<BoundColumn>> = Arc::new(
                        (0..num_cols)
                            .map(|i| BoundColumn {
                                name: format!("column{}", i + 1),
                                expr: ast::Expr::Literal(ast::Literal::Numeric(i.to_string())),
                                is_explicit_alias: false,
                            })
                            .collect(),
                    );
                    for row in rows.iter_mut() {
                        for (index, expr) in row.iter_mut().enumerate() {
                            ctx.bind_expr_with_expected_type(
                                expr,
                                &scope,
                                expected_types.get(index).and_then(Option::as_deref),
                            )?;
                        }
                    }
                    Ok((bound_columns, scope))
                }
            }
        })
    }

    /// Bind expressions in the SELECT list.
    fn bind_select_list(
        &mut self,
        columns: &mut [ast::ResultColumn],
        scope: &BindScope,
    ) -> Result<()> {
        for col in columns.iter_mut() {
            match col {
                ast::ResultColumn::Expr(expr, _) => {
                    self.bind_expr(expr, scope)?;
                }
                // Star and TableStar don't contain expressions to bind
                ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => {}
            }
        }
        Ok(())
    }

    /// Bind WINDOW definition expressions (PARTITION BY, ORDER BY).
    fn bind_window_defs(
        &mut self,
        window_defs: &mut [ast::WindowDef],
        scope: &BindScope,
    ) -> Result<()> {
        for def in window_defs.iter_mut() {
            for expr in &mut def.window.partition_by {
                self.bind_expr(expr, scope)?;
            }
            for sorted_col in &mut def.window.order_by {
                self.bind_expr(&mut sorted_col.expr, scope)?;
            }
        }
        Ok(())
    }

    /// Inline trivial subqueries like `(SELECT alias_name)` by resolving
    /// the inner expression against the current alias list. This avoids
    /// creating correlated subqueries for ORDER BY / HAVING expressions
    /// that are really just alias references wrapped in a subquery.
    /// Inline trivial subqueries like `(SELECT alias_name)` by resolving
    /// the inner expression against the current alias list. Only inlines if
    /// the name ONLY matches an alias and NOT a source column (SQLite gives
    /// source columns priority inside subquery context).
    fn try_inline_trivial_subquery(&self, expr: &mut ast::Expr, scope: &BindScope) {
        // Inline at any depth: e.g. `HAVING (SELECT s) > 15` wraps the trivial
        // subquery inside a comparison.
        let _ = walk_expr_mut(expr, &mut |e: &mut ast::Expr| {
            self.try_inline_trivial_subquery_at(e, scope);
            Ok(WalkControl::Continue)
        });
    }

    fn try_inline_trivial_subquery_at(&self, expr: &mut ast::Expr, scope: &BindScope) {
        if let ast::Expr::Subquery(select) = expr {
            // Only inline if there's no FROM, no WHERE, no compounds, no LIMIT
            if select.with.is_none()
                && select.body.compounds.is_empty()
                && select.order_by.is_empty()
                && select.limit.is_none()
            {
                if let ast::OneSelect::Select {
                    columns,
                    from: None,
                    where_clause: None,
                    group_by: None,
                    ..
                } = &select.body.select
                {
                    if columns.len() == 1 {
                        // An implicit column name (preserved SQL text) is not a
                        // user alias, so it doesn't disqualify inlining.
                        if let ast::ResultColumn::Expr(inner_expr, alias) = &columns[0] {
                            if alias.as_ref().is_some_and(|a| a.is_explicit()) {
                                return;
                            }
                            if let ast::Expr::Id(name) = inner_expr.as_ref() {
                                // Only inline if the name doesn't match any source column.
                                // If a source column exists, the subquery should
                                // resolve it as a correlated column reference, not an alias.
                                if scope
                                    .find_column_unqualified(name.as_str())
                                    .ok()
                                    .flatten()
                                    .is_none()
                                {
                                    if let Some(alias_expr) = self.resolve_alias(name.as_str()) {
                                        *expr = alias_expr;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Replace a numeric literal (e.g. `1`, `2`) with the corresponding
    /// SELECT result column expression, mirroring SQLite: only positive
    /// integer literals are column references (floats stay constant
    /// expressions); an explicit COLLATE or a single-element parenthesized
    /// wrapper is looked through; `+2` counts as an ordinal while `-2` is out
    /// of range.
    fn replace_column_number(&self, expr: &mut ast::Expr) -> Result<()> {
        self.replace_column_number_inner(expr, "ORDER BY")
    }

    fn replace_column_number_inner(&self, expr: &mut ast::Expr, clause_name: &str) -> Result<()> {
        match expr {
            ast::Expr::Collate(inner, _) => {
                return self.replace_column_number_inner(inner, clause_name);
            }
            ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
                let inner = exprs[0].as_mut();
                return self.replace_column_number_inner(inner, clause_name);
            }
            _ => {}
        }

        let num_str = match expr {
            ast::Expr::Literal(ast::Literal::Numeric(num)) => Some(num.clone()),
            ast::Expr::Unary(ast::UnaryOperator::Positive, inner) => {
                if let ast::Expr::Literal(ast::Literal::Numeric(num)) = inner.as_ref() {
                    Some(num.clone())
                } else {
                    None
                }
            }
            ast::Expr::Unary(ast::UnaryOperator::Negative, inner) => {
                if let ast::Expr::Literal(ast::Literal::Numeric(num)) = inner.as_ref() {
                    if num.parse::<i32>().is_ok() {
                        crate::bail_parse_error!(
                            "1st {} term out of range - should be between 1 and {}",
                            clause_name,
                            self.aliases().len()
                        );
                    }
                }
                None
            }
            _ => None,
        };
        if let Some(num) = num_str {
            // Mirroring SQLite's sqlite3ExprIsInteger, only literals that fit
            // a 32-bit int count as column positions; larger integers (and
            // floats) are ordinary constant expressions.
            if let Ok(column_number) = num.parse::<i32>() {
                let aliases = self.aliases();
                if column_number <= 0 || column_number as usize > aliases.len() {
                    crate::bail_parse_error!(
                        "1st {} term out of range - should be between 1 and {}",
                        clause_name,
                        aliases.len()
                    );
                }
                *expr = aliases[column_number as usize - 1].expr.clone();
            }
        }
        Ok(())
    }

    /// Bind GROUP BY expressions and HAVING clause.
    fn bind_group_by(&mut self, group_by: &mut ast::GroupBy, scope: &BindScope) -> Result<()> {
        // GROUP BY expressions in a correlated subquery cannot reference the
        // outer query scope. Raise the frame floor so resolution can't see
        // enclosing scopes (subqueries inside the GROUP BY expr still push and
        // see frames above the floor), while error messages can still name
        // outer tables correctly.
        let saved_floor = self.outer_frame_floor;
        self.outer_frame_floor = self.outer_query_frames.len();
        let group_result: Result<()> = (|| {
            for expr in &mut group_by.exprs {
                self.replace_column_number_inner(expr, "GROUP BY")?;
                self.bind_expr(expr, scope)?;
            }
            Ok(())
        })();
        self.outer_frame_floor = saved_floor;
        group_result?;
        if let Some(having) = &mut group_by.having {
            // Before alias resolution replaces identifiers with their
            // underlying expressions, reject identifiers inside an aggregate's
            // arguments that resolve to an aggregate alias, matching SQLite's
            // "misuse of aliased aggregate" (NC_AllowAgg in resolve.c).
            self.check_aliased_aggregate_misuse(having)?;
            self.with_phase(BindPhase::AliasFirst, |ctx| {
                ctx.try_inline_trivial_subquery(having, scope);
                ctx.bind_expr(having, scope)
            })?;
        }
        Ok(())
    }

    /// Reject `HAVING agg(... alias ...)` where `alias` names an aggregate
    /// result column (e.g. `SELECT min(x) AS m ... HAVING max(m+5) < 10`).
    fn check_aliased_aggregate_misuse(&self, expr: &ast::Expr) -> Result<()> {
        let expr_contains_aggregate = |e: &ast::Expr| {
            let mut found = false;
            let _ = walk_expr(e, &mut |n: &ast::Expr| {
                match n {
                    ast::Expr::FunctionCall { name, args, .. } => {
                        if matches!(
                            Func::resolve_function(name.as_str(), args.len()),
                            Ok(Some(Func::Agg(_)))
                        ) {
                            found = true;
                            return Ok(WalkControl::SkipChildren);
                        }
                    }
                    ast::Expr::FunctionCallStar { name, .. } => {
                        if matches!(
                            Func::resolve_function(name.as_str(), 0),
                            Ok(Some(Func::Agg(_)))
                        ) {
                            found = true;
                            return Ok(WalkControl::SkipChildren);
                        }
                    }
                    _ => {}
                }
                Ok(WalkControl::Continue)
            });
            found
        };
        walk_expr(expr, &mut |e: &ast::Expr| {
            let is_agg = match e {
                ast::Expr::FunctionCall { name, args, .. } => matches!(
                    Func::resolve_function(name.as_str(), args.len()),
                    Ok(Some(Func::Agg(_)))
                ),
                ast::Expr::FunctionCallStar { name, .. } => matches!(
                    Func::resolve_function(name.as_str(), 0),
                    Ok(Some(Func::Agg(_)))
                ),
                _ => false,
            };
            if !is_agg {
                return Ok(WalkControl::Continue);
            }
            if let ast::Expr::FunctionCall { args, .. } = e {
                for arg in args.iter() {
                    walk_expr(arg, &mut |n: &ast::Expr| {
                        if let ast::Expr::Id(id) = n {
                            let normalized = normalize_ident(id.as_str());
                            for bc in self.aliases().iter() {
                                if bc.is_explicit_alias
                                    && bc.name.eq_ignore_ascii_case(&normalized)
                                    && expr_contains_aggregate(&bc.expr)
                                {
                                    crate::bail_parse_error!(
                                        "misuse of aliased aggregate {}",
                                        normalized
                                    );
                                }
                            }
                        }
                        Ok(WalkControl::Continue)
                    })?;
                }
            }
            Ok(WalkControl::SkipChildren)
        })?;
        Ok(())
    }

    /// Expand `Star` and `TableStar` result columns in-place (RETURNING only —
    /// SELECT lists keep stars unexpanded and go through the planner's
    /// `select_star` fast path instead).
    ///
    /// After this, the `columns` vec contains only `ResultColumn::Expr` entries.
    /// Handles USING dedup, hidden columns, semi/anti-join filtering, and
    /// right_join_swapped ordering — matching the planner's `select_star`.
    fn expand_stars(
        &mut self,
        columns: &mut Vec<ast::ResultColumn>,
        scope: &BindScope,
    ) -> Result<()> {
        let mut expanded = Vec::with_capacity(columns.len());
        for col in columns.drain(..) {
            match col {
                ast::ResultColumn::Star => {
                    let table_iter: Vec<&ScopeTable> = if scope.right_join_swapped {
                        scope.tables.iter().rev().collect()
                    } else {
                        scope.tables.iter().collect()
                    };
                    for st in table_iter {
                        // Semi/anti-join tables don't contribute to SELECT *
                        if st.join_info.as_ref().is_some_and(|ji| ji.is_semi_or_anti()) {
                            continue;
                        }
                        // If this table's identifier appears more than once in
                        // the FROM clause, expanding * would produce ambiguous
                        // column references (matches SQLite). Columns
                        // deduplicated by USING/NATURAL are not ambiguous.
                        let has_duplicate_identifier = scope
                            .tables
                            .iter()
                            .filter(|t| t.identifier == st.identifier)
                            .count()
                            > 1;
                        if has_duplicate_identifier {
                            let using_cols: Vec<&str> = scope
                                .tables
                                .iter()
                                .filter(|t| t.identifier == st.identifier)
                                .filter_map(|t| t.join_info.as_ref())
                                .flat_map(|ji| ji.using.iter().map(|u| u.as_str()))
                                .collect();
                            for col_ref in st.table.columns() {
                                if col_ref.is_hidden {
                                    continue;
                                }
                                let in_using = using_cols
                                    .iter()
                                    .any(|u| u.eq_ignore_ascii_case(col_ref.name));
                                if !in_using {
                                    crate::bail_parse_error!(
                                        "ambiguous column name: {}.{}",
                                        st.identifier,
                                        col_ref.name
                                    );
                                }
                            }
                        }
                        for col_ref in st.table.columns() {
                            if col_ref.is_hidden {
                                continue;
                            }
                            // USING dedup: skip columns from right table that are in USING
                            if let Some(ji) = &st.join_info {
                                if ji
                                    .using
                                    .iter()
                                    .any(|u| u.as_str().eq_ignore_ascii_case(col_ref.name))
                                {
                                    continue;
                                }
                            }
                            self.tracking.record_column(st.internal_id, col_ref.idx);
                            expanded.push(ast::ResultColumn::Expr(
                                Box::new(ast::Expr::Column {
                                    database: None,
                                    table: st.internal_id,
                                    column: col_ref.idx,
                                    is_rowid_alias: col_ref.is_rowid_alias,
                                }),
                                Some(ast::As::As(ast::Name::exact(col_ref.name.to_string()))),
                            ));
                        }
                    }
                }
                ast::ResultColumn::TableStar(ref name) => {
                    let normalized = normalize_ident(name.as_str());
                    if let Some(st) = scope
                        .tables
                        .iter()
                        .find(|t| t.identifier.eq_ignore_ascii_case(&normalized))
                    {
                        for col_ref in st.table.columns() {
                            if col_ref.is_hidden {
                                continue;
                            }
                            self.tracking.record_column(st.internal_id, col_ref.idx);
                            expanded.push(ast::ResultColumn::Expr(
                                Box::new(ast::Expr::Column {
                                    database: None,
                                    table: st.internal_id,
                                    column: col_ref.idx,
                                    is_rowid_alias: col_ref.is_rowid_alias,
                                }),
                                Some(ast::As::As(ast::Name::exact(col_ref.name.to_string()))),
                            ));
                        }
                    } else {
                        // Table not found — leave as-is, planner will error
                        expanded.push(col);
                    }
                }
                other => expanded.push(other),
            }
        }
        *columns = expanded;
        Ok(())
    }

