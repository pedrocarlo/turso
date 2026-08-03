    fn bind_from(&mut self, from: &mut ast::FromClause) -> Result<BindScope> {
        use super::plan::JoinType as PlanJoinType;

        let mut tables: Vec<ScopeTable> = Vec::new();
        let mut right_join_swapped = false;

        tables.push(self.resolve_select_table(&mut from.select, None, 0)?);
        for join in &mut from.joins {
            // Build a temporary scope from tables accumulated so far, so that
            // table function arguments can reference previously-joined tables.
            let lateral_scope = BindScope {
                tables: tables.clone(),
                right_join_swapped: false,
            };
            let mut st =
                self.resolve_select_table(&mut join.table, Some(&lateral_scope), tables.len())?;

            // SQLite allows duplicate table names/aliases in FROM clauses.
            // Ambiguity is detected later during column resolution.

            let (is_outer, is_full_outer, is_right, is_cross, is_natural) = match &join.operator {
                ast::JoinOperator::TypedJoin(Some(jt)) => {
                    let is_left = jt.contains(ast::JoinType::LEFT);
                    let is_right = jt.contains(ast::JoinType::RIGHT);
                    let is_outer = jt.contains(ast::JoinType::OUTER) || is_left;
                    let is_full = (is_left && is_right) || (is_outer && !is_left && !is_right);
                    let is_cross = jt.contains(ast::JoinType::CROSS);
                    let is_natural = jt.contains(ast::JoinType::NATURAL);
                    (
                        is_outer && !is_full,
                        is_full,
                        is_right && !is_left && !is_full,
                        is_cross,
                        is_natural,
                    )
                }
                _ => (false, false, false, false, false),
            };

            // NATURAL JOIN: find common columns and rewrite constraint to USING
            if is_natural {
                if join.constraint.is_some() {
                    crate::bail_parse_error!("a NATURAL join may not have an ON or USING clause");
                }
                // SQLite doesn't use HIDDEN columns for NATURAL joins:
                // https://www3.sqlite.org/src/info/ab09ef427181130b
                // The USING list uses the left table's column name spelling,
                // matching parse_join. No common columns = cross join.
                let right_table: &dyn BindTable = st.table.as_ref();
                let mut common_cols: Vec<ast::Name> = Vec::new();
                for right_col in right_table.columns() {
                    if right_col.is_hidden {
                        continue;
                    }
                    let mut found: Option<String> = None;
                    for left_st in &tables {
                        let left_table: &dyn BindTable = left_st.table.as_ref();
                        for left_col in left_table.columns() {
                            if left_col.is_hidden {
                                continue;
                            }
                            if left_col.name.eq_ignore_ascii_case(right_col.name) {
                                found = Some(left_col.name.to_string());
                                break;
                            }
                        }
                        if found.is_some() {
                            break;
                        }
                    }
                    if let Some(left_name) = found {
                        common_cols.push(ast::Name::exact(left_name));
                    }
                }
                if !common_cols.is_empty() {
                    join.constraint = Some(JoinConstraint::Using(common_cols));
                }
            }

            // Determine USING columns from (possibly rewritten) constraint
            let using_cols = match &join.constraint {
                Some(JoinConstraint::Using(cols)) => cols.to_vec(),
                _ => vec![],
            };

            // RIGHT JOIN: rewrite as LEFT JOIN by swapping tables.
            // Push the right table first, then swap it to the front so it
            // becomes the driving table. The originally-left table gets the
            // LeftOuter join info.
            if is_right {
                if tables.len() > 1 {
                    crate::bail_parse_error!(
                        "RIGHT JOIN following another join is not yet supported. \
                         Try rewriting as LEFT JOIN or using a subquery."
                    );
                }
                // Push right table, then swap so it's first
                tables.push(st);
                let last = tables.len() - 1;
                tables.swap(0, last);
                // The originally-left table (now at last position) gets the outer flag
                tables[last].join_info = Some(JoinInfo {
                    join_type: PlanJoinType::LeftOuter,
                    using: using_cols.clone(),
                    no_reorder: false,
                });
                right_join_swapped = true;
            } else {
                let plan_join_type = if is_full_outer {
                    PlanJoinType::FullOuter
                } else if is_outer {
                    PlanJoinType::LeftOuter
                } else {
                    PlanJoinType::Inner
                };
                st.join_info = Some(JoinInfo {
                    join_type: plan_join_type,
                    using: using_cols,
                    no_reorder: is_cross,
                });
                tables.push(st);
            }
        }

        let mut scope = BindScope {
            tables,
            right_join_swapped,
        };
        self.bind_index_method_patterns(&mut scope)?;
        self.bind_index_expressions(&mut scope);

        // Bind ON expressions against the complete scope
        for join in &mut from.joins {
            match &mut join.constraint {
                Some(JoinConstraint::On(expr)) => {
                    self.bind_expr(expr, &scope)?;
                }
                // USING column usage is marked by fold_join_constraints when
                // the equality predicates are synthesized, matching parse_join.
                Some(JoinConstraint::Using(_)) | None => {}
            }
        }

        // Second pass: re-bind table function args that may have been left
        // unresolved due to forward references (e.g. FROM func(s.col), s).
        // Now that all tables are in scope, resolve any remaining Expr::Qualified/Id.
        if let ast::SelectTable::TableCall(_, args, _) = from.select.as_mut() {
            for arg in args.iter_mut() {
                self.bind_expr(arg, &scope)?;
            }
        }
        for join in &mut from.joins {
            if let ast::SelectTable::TableCall(_, args, _) = join.table.as_mut() {
                for arg in args.iter_mut() {
                    self.bind_expr(arg, &scope)?;
                }
            }
        }

        Ok(scope)
    }

    /// Resolve every custom index-method pattern against the table reference
    /// it may optimize. The optimizer receives only this bound form.
    fn bind_index_method_patterns(&mut self, scope: &mut BindScope) -> Result<()> {
        for scope_table in &mut scope.tables {
            let ScopeTableSource::Table(table) = &scope_table.source else {
                continue;
            };
            let indexes = self
                .resolver
                .with_schema(scope_table.database_id, |schema| {
                    schema.indexes.get(table.get_name()).cloned()
                });
            let Some(indexes) = indexes else {
                continue;
            };

            let mut bound_patterns = Vec::new();
            for index in indexes {
                let Some(index_method) = &index.index_method else {
                    continue;
                };
                if index.is_backing_btree_index() {
                    continue;
                }
                let raw_patterns = index_method.definition().patterns.to_vec();
                for (pattern_idx, raw_pattern) in raw_patterns.iter().enumerate() {
                    bound_patterns.push(self.bind_index_method_pattern(
                        raw_pattern,
                        scope_table,
                        index.name.clone(),
                        pattern_idx,
                    )?);
                }
            }
            scope_table.bound_index_method_patterns = bound_patterns;
        }
        Ok(())
    }

    fn bind_index_expressions(&self, scope: &mut BindScope) {
        for scope_table in &mut scope.tables {
            let ScopeTableSource::Table(table) = &scope_table.source else {
                continue;
            };
            scope_table.bound_index_expressions = bind_table_index_expressions(
                self.resolver,
                scope_table.database_id,
                table.get_name(),
                scope_table.internal_id,
            );
        }
    }

    fn bind_index_method_pattern(
        &mut self,
        raw_pattern: &ast::Select,
        target: &ScopeTable,
        index_name: String,
        pattern_idx: usize,
    ) -> Result<super::plan::BoundIndexMethodPattern> {
        let mut pattern = raw_pattern.clone();
        if pattern.with.is_some() || !pattern.body.compounds.is_empty() {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' must be a single SELECT"
            )));
        }

        let ast::OneSelect::Select {
            columns,
            from: Some(ast::FromClause { select, joins }),
            distinctness: None,
            where_clause,
            group_by: None,
            window_clause,
        } = &mut pattern.body.select
        else {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' has an unsupported SELECT body"
            )));
        };
        if !joins.is_empty() || !window_clause.is_empty() {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' cannot contain joins or windows"
            )));
        }
        let ast::SelectTable::Table(pattern_table_name, _, _) = select.as_ref() else {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' must read one table"
            )));
        };
        let ScopeTableSource::Table(target_table) = &target.source else {
            unreachable!("index method patterns only belong to schema tables")
        };
        let target_table_name = target_table.get_name();
        if !pattern_table_name
            .name
            .as_str()
            .eq_ignore_ascii_case(target_table_name)
        {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' reads '{}', expected '{target_table_name}'",
                pattern_table_name.name.as_str()
            )));
        }

        let mut pattern_table = target.clone();
        pattern_table.identifier = normalize_ident(pattern_table_name.name.as_str());
        pattern_table.bound_index_method_patterns.clear();
        let pattern_scope = BindScope {
            tables: vec![pattern_table],
            right_join_swapped: false,
        };

        let mut binder = BindContext::new(self.resolver, &mut *self.id_gen);
        let aliases = Arc::new(binder.extract_bound_columns(columns, &pattern_scope, &[])?);
        binder.set_aliases(Arc::clone(&aliases));
        binder.with_phase(BindPhase::NoAliases, |binder| {
            binder.bind_select_list(columns, &pattern_scope)
        })?;
        if let Some(where_clause) = where_clause {
            binder.with_phase(BindPhase::AliasFirst, |binder| {
                binder.bind_expr(where_clause, &pattern_scope)
            })?;
        }
        binder.with_phase(BindPhase::AliasFirst, |binder| {
            for order_by in &mut pattern.order_by {
                binder.bind_expr(&mut order_by.expr, &pattern_scope)?;
            }
            Ok(())
        })?;
        if let Some(limit) = pattern.limit.as_mut() {
            let empty_scope = BindScope::empty();
            binder.bind_expr(&mut limit.expr, &empty_scope)?;
            if let Some(offset) = limit.offset.as_mut() {
                binder.bind_expr(offset, &empty_scope)?;
            }
        }
        if !binder.subquery_bindings.is_empty() || !binder.derived_bindings.is_empty() {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' cannot contain subqueries"
            )));
        }
        drop(binder);

        let ast::OneSelect::Select {
            columns,
            where_clause,
            ..
        } = pattern.body.select
        else {
            unreachable!("index method pattern shape was validated above")
        };
        Ok(super::plan::BoundIndexMethodPattern {
            index_name,
            pattern_idx,
            columns,
            where_clause,
            order_by: pattern.order_by,
            limit: pattern.limit,
        })
    }

