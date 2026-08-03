    // ── UPDATE binding ──────────────────────────────────────────────────

    /// Bind an UPDATE statement, resolving all name references in-place.
    pub fn bind_update(
        &mut self,
        update: &mut ast::Update,
        database_id: usize,
        table: Arc<Table>,
    ) -> Result<BoundUpdate> {
        let or_conflict = update.or_conflict.take();
        self.with_query(|ctx| {
            // 1. Bind CTEs from WITH clause
            if let Some(with) = &mut update.with {
                ctx.bind_cte(with)?;
            }

            // 2. Build scope with target table
            let target_scope = ctx.build_table_scope(
                &update.tbl_name.name,
                update.tbl_name.alias.as_ref(),
                update.indexed.clone(),
                database_id,
            )?;

            // 3. Bind the UPDATE ... FROM clause (if present). Its JOIN ON
            //    constraints bind against the FROM tables only — they cannot
            //    reference the target table.
            let from_scope = match update.from.as_mut() {
                Some(from) => Some(ctx.bind_from(from)?),
                None => None,
            };

            // 4. Merge target + FROM tables into the read scope used by SET
            //    and WHERE expressions.
            let mut read_scope = target_scope.clone();
            if let Some(fs) = &from_scope {
                read_scope.tables.extend(fs.tables.iter().cloned());
                read_scope.right_join_swapped = fs.right_join_swapped;
            }

            // 5. Bind SET expressions (column-name → index mapping stays in
            //    the planner via collect_update_set_clauses).
            ctx.with_phase(BindPhase::NoAliases, |ctx| {
                for set in update.sets.iter_mut() {
                    bind_update_set(ctx, set, &read_scope, &table)?;
                }
                Ok(())
            })?;

            // 6. Bind WHERE clause against the merged scope
            if let Some(where_expr) = &mut update.where_clause {
                ctx.with_phase(BindPhase::NoAliases, |ctx| {
                    ctx.bind_expr(where_expr, &read_scope)
                })?;
            }

            // 7. Bind RETURNING. SQLite resolves RETURNING columns for an
            //    aliased UPDATE target through the base table name, not the
            //    alias, and FROM tables are not visible.
            let mut returning_scope = target_scope.clone();
            returning_scope.tables[0].identifier = normalize_ident(update.tbl_name.name.as_str());
            ctx.bind_returning(&mut update.returning, &returning_scope)?;

            // 8. Bind ORDER BY
            for sort_col in &mut update.order_by {
                ctx.bind_expr(&mut sort_col.expr, &read_scope)?;
            }

            // 9. Bind LIMIT/OFFSET
            if let Some(limit) = update.limit.as_mut() {
                let empty = BindScope::empty();
                ctx.bind_expr(&mut limit.expr, &empty)?;
                if let Some(offset) = limit.offset.as_mut() {
                    ctx.bind_expr(offset, &empty)?;
                }
            }

            // 10. Extract CTE definitions in definition order (critical for
            //     referenced_cte_indices correctness).
            let cte_definitions: Vec<(String, CteEntry)> = if let Some(with) = &update.with {
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

            Ok(BoundUpdate {
                target_scope,
                from_scope,
                tracking: std::mem::take(&mut ctx.tracking),
                subquery_bindings: std::mem::take(&mut ctx.subquery_bindings),
                derived_bindings: std::mem::take(&mut ctx.derived_bindings),
                cte_definitions,
                database_id,
                table,
                or_conflict,
            })
        })
    }

    /// Build a single-table scope for DML statements (UPDATE, DELETE).
    /// `database_id` specifies which attached database to search (0 = main).
    ///
    /// DML targets are always schema tables — a WITH-clause CTE never shadows
    /// the target (matching SQLite, where `WITH t AS (...) DELETE FROM t`
    /// modifies the real table t).
    fn build_table_scope(
        &mut self,
        table_name: &ast::Name,
        alias: Option<&ast::Name>,
        indexed: Option<ast::Indexed>,
        database_id: usize,
    ) -> Result<BindScope> {
        let normalized = normalize_ident(table_name.as_str());
        let identifier = alias
            .map(|a| normalize_ident(a.as_str()))
            .unwrap_or_else(|| normalized.clone());

        // Schema lookup (uses the specified database for attached DB support)
        let schema_table = self
            .resolver
            .with_schema(database_id, |s| s.get_table(&normalized))
            .ok_or_else(|| crate::LimboError::ParseError(format!("no such table: {normalized}")))?;

        let mut scope = BindScope {
            tables: vec![ScopeTable {
                identifier,
                internal_id: self.id_gen.next_table_id(),
                source: ScopeTableSource::Table(schema_table.clone()),
                table: schema_table,
                join_info: None,
                database_id,
                indexed,
                bound_index_method_patterns: Vec::new(),
                bound_index_expressions: Vec::new(),
            }],
            right_join_swapped: false,
        };
        self.bind_index_method_patterns(&mut scope)?;
        self.bind_index_expressions(&mut scope);
        Ok(scope)
    }

    /// Bind a RETURNING clause: expand stars, bind expressions, return bound columns.
    fn bind_returning(
        &mut self,
        returning: &mut Vec<ast::ResultColumn>,
        scope: &BindScope,
    ) -> Result<Vec<BoundColumn>> {
        if returning.is_empty() {
            return Ok(vec![]);
        }

        // Expand Star/TableStar in RETURNING
        self.expand_stars(returning, scope)?;

        let mut result = Vec::with_capacity(returning.len());
        for rc in returning.iter_mut() {
            match rc {
                ast::ResultColumn::Expr(expr, alias) => {
                    self.bind_expr(expr, scope)?;
                    let name = alias
                        .as_ref()
                        .map(|a| a.name().as_str().to_string())
                        .unwrap_or_else(|| Self::infer_column_name(expr));
                    result.push(BoundColumn {
                        name,
                        expr: expr.as_ref().clone(),
                        is_explicit_alias: alias.is_some(),
                    });
                }
                ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => {
                    unreachable!("Star/TableStar should be expanded before binding RETURNING")
                }
            }
        }

        Ok(result)
    }

