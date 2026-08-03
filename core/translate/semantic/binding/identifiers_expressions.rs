    fn bind_identifier(&mut self, expr: &mut ast::Expr, scope: &BindScope) -> Result<()> {
        if self.bind_trigger_column(expr)? {
            return Ok(());
        }

        match expr {
            ast::Expr::Id(id) => {
                let resolved = match self.phase() {
                    BindPhase::NoAliases => self.resolve_unqualified_column(id.as_str(), scope)?,
                    BindPhase::TableFirst => self
                        .resolve_unqualified_column(id.as_str(), scope)?
                        .or_else(|| self.resolve_alias(id.as_str()))
                        .or_else(|| self.resolve_outer_alias(id.as_str())),
                    BindPhase::AliasFirst => {
                        if let Some(alias) = self.resolve_alias(id.as_str()) {
                            // Even though the alias matched, check if the name is
                            // ambiguous as a table column. SQLite errors on
                            // ORDER BY value when multiple tables have 'value'.
                            scope.find_column_unqualified(id.as_str())?;
                            Some(alias)
                        } else {
                            self.resolve_unqualified_column(id.as_str(), scope)?
                                .or_else(|| self.resolve_outer_alias(id.as_str()))
                        }
                    }
                };

                if let Some(resolved) = resolved {
                    *expr = resolved;
                    return Ok(());
                }

                // SQLite DQS misfeature: double-quoted identifiers fall back
                // to string literals only when DQS is enabled.
                if id.quoted_with('"') && self.resolver.dqs_dml.is_enabled() {
                    *expr = ast::Expr::Literal(ast::Literal::String(id.as_literal()));
                } else if self.allow_unbound {
                    // Leave as-is (e.g. EXCLUDED pseudo-table refs in UPSERT)
                } else {
                    crate::bail_parse_error!("no such column: {}", id.as_str());
                }
            }
            ast::Expr::Qualified(tbl, col) => {
                if let Some(resolved) =
                    self.resolve_qualified_column(tbl.as_str(), col.as_str(), scope)?
                {
                    *expr = resolved;
                } else if self.allow_unbound {
                    // Leave as-is (e.g. EXCLUDED.col in UPSERT)
                } else {
                    // Check whether the table itself exists to give a better error.
                    // Also check CTEs and outer scopes — a CTE name that isn't
                    // in FROM still counts as a "known table" for error messages.
                    let tbl_normalized = normalize_ident(tbl.as_str());
                    let table_exists = scope.find_table_by_identifier(tbl.as_str()).is_some()
                        || self.ctes.contains_key(&tbl_normalized)
                        || self
                            .all_outer_scopes_iter()
                            .any(|s| s.find_table_by_identifier(tbl.as_str()).is_some());
                    if table_exists {
                        crate::bail_parse_error!(
                            "no such column: {}.{}",
                            tbl.as_str(),
                            col.as_str()
                        );
                    }
                    // Dot-notation fallback for struct/union field access:
                    // for `a.b`, if no table `a` exists anywhere, try
                    // a=column, b=struct field (table references win).
                    let field_name = normalize_ident(col.as_str());
                    if let Some((table_id, col_idx, is_rowid_alias, td)) =
                        self.find_custom_type_column_in_scope(scope, &tbl_normalized)?
                    {
                        *expr = Self::make_field_access_expr(
                            table_id,
                            col_idx,
                            is_rowid_alias,
                            &field_name,
                            td,
                        )?;
                        self.tracking.record_column(table_id, col_idx);
                        return Ok(());
                    }
                    crate::bail_parse_error!("no such table: {}", tbl_normalized);
                }
            }
            ast::Expr::DoublyQualified(db_name, tbl_name, col_name) => {
                let qname = ast::QualifiedName {
                    db_name: Some(db_name.clone()),
                    name: tbl_name.clone(),
                    alias: None,
                };
                // In trigger context, cross-database DoublyQualified references
                // (e.g. aux.ref_t.v) should not be resolved at compile time.
                // SQLite defers this to runtime with "no such column".
                // We check the db name directly because resolve_database_id
                // would error with "trigger cannot reference objects in database X".
                if let Some(ref ctx) = self.resolver.trigger_context {
                    let db_name_normalized = normalize_ident(db_name.as_str());
                    let trigger_db_name = if ctx.database_id() == crate::MAIN_DB_ID {
                        "main".to_string()
                    } else {
                        self.resolver
                            .get_database_name_by_index(ctx.database_id())
                            .unwrap_or_else(|| "main".to_string())
                            .to_lowercase()
                    };
                    if !db_name_normalized.eq_ignore_ascii_case(&trigger_db_name) {
                        // Cross-database ref in trigger — error at runtime with
                        // "no such column" matching SQLite behavior.
                        crate::bail_parse_error!(
                            "no such column: {}.{}.{}",
                            db_name.as_str(),
                            tbl_name.as_str(),
                            col_name.as_str()
                        );
                    }
                }
                if self.allow_unbound {
                    return Ok(());
                }

                // `a.b.c` resolution order (DuckDB-style precedence, mirrors
                // bind_and_rewrite_expr):
                //   1. a=database, b=table,  c=column
                //   2. a=table,    b=column, c=struct/union field
                //   3. a=column,   b=field,  c=sub-field
                let db_resolution = self.resolver.resolve_database_id(&qname);
                if let Ok(database_id) = db_resolution {
                    // The interpretation only holds if the named database
                    // actually contains the table (mirrors bind_and_rewrite:
                    // `temp.t1.y` must not resolve through a main-schema t1
                    // that happens to be in the FROM clause).
                    let table_in_db = self.resolver.with_schema(database_id, |schema| {
                        schema
                            .get_table(&normalize_ident(tbl_name.as_str()))
                            .is_some()
                    });
                    if table_in_db {
                        if let Some(resolved) = self.resolve_qualified_column(
                            tbl_name.as_str(),
                            col_name.as_str(),
                            scope,
                        )? {
                            match resolved {
                                ast::Expr::Column {
                                    table,
                                    column,
                                    is_rowid_alias,
                                    ..
                                } => {
                                    *expr = ast::Expr::Column {
                                        database: Some(database_id),
                                        table,
                                        column,
                                        is_rowid_alias,
                                    };
                                }
                                other => *expr = other,
                            }
                            return Ok(());
                        }
                    }
                }

                // db.table.column failed — try table.column.field for
                // struct/union access.
                let normalized_tbl_name = normalize_ident(db_name.as_str());
                let normalized_col = normalize_ident(tbl_name.as_str());
                let field_name = normalize_ident(col_name.as_str());
                if let Some(st) = scope.find_table_by_identifier(&normalized_tbl_name) {
                    if let ScopeTableSource::Table(table) = &st.source {
                        let cols = table.columns();
                        if let Some(col_idx) = cols.iter().position(|c| {
                            c.name
                                .as_ref()
                                .is_some_and(|n| n.eq_ignore_ascii_case(&normalized_col))
                        }) {
                            let col = &cols[col_idx];
                            let type_def =
                                self.resolver.schema().get_type_def_unchecked(&col.ty_str);
                            let is_struct_or_union = type_def
                                .map(|td| td.is_struct() || td.is_union())
                                .unwrap_or(false);
                            if is_struct_or_union {
                                let internal_id = st.internal_id;
                                let is_rowid_alias = col.is_rowid_alias();
                                *expr = Self::make_field_access_expr(
                                    internal_id,
                                    col_idx,
                                    is_rowid_alias,
                                    &field_name,
                                    type_def.unwrap(),
                                )?;
                                self.tracking.record_column(internal_id, col_idx);
                                return Ok(());
                            } else {
                                // Column exists but is not a struct/union type
                                return Err(crate::LimboError::ParseError(format!(
                                    "column '{normalized_col}' is not a STRUCT or UNION type; \
                                     cannot access field '{field_name}'"
                                )));
                            }
                        }
                    }
                }

                // Fallback (3): column.field.subfield for nested struct/union
                // access (e.g. data.telegram.chat_id).
                if let Some(nested_expr) = self.try_resolve_nested_field_access_in_scope(
                    scope,
                    &normalized_tbl_name,
                    &normalized_col,
                    &field_name,
                )? {
                    *expr = nested_expr;
                    return Ok(());
                }

                crate::bail_parse_error!(
                    "no such column: {}.{}.{}",
                    db_name.as_str(),
                    tbl_name.as_str(),
                    col_name.as_str()
                );
            }
            _ => unreachable!("bind_identifier only handles identifier nodes"),
        }

        Ok(())
    }

    fn bind_subquery_expr(
        &mut self,
        select: &mut ast::Select,
        scope: &BindScope,
    ) -> Result<BoundSelect> {
        #[expect(clippy::arc_with_non_send_sync)]
        self.append_outer_query_scope(Arc::new(scope.clone()), Arc::clone(&self.aliases));
        let result = self.bind_select(select);
        self.pop_outer_query_scope();
        result
    }

    /// Populate `shared_subqueries` with scalar subqueries that appear in both
    /// GROUP BY and the SELECT list (compared on the raw, pre-bind AST).
    fn collect_shared_subqueries(
        &mut self,
        columns: &[ast::ResultColumn],
        group_by: Option<&ast::GroupBy>,
    ) {
        self.shared_subqueries.clear();
        let Some(group_by) = group_by else {
            return;
        };
        let mut in_group_by: Vec<ast::Expr> = Vec::new();
        for expr in &group_by.exprs {
            let _ = walk_expr(expr, &mut |e: &ast::Expr| {
                if matches!(e, ast::Expr::Subquery(_)) {
                    in_group_by.push(e.clone());
                }
                Ok(WalkControl::Continue)
            });
        }
        if in_group_by.is_empty() {
            return;
        }
        for col in columns {
            let ast::ResultColumn::Expr(expr, _) = col else {
                continue;
            };
            let _ = walk_expr(expr.as_ref(), &mut |e: &ast::Expr| {
                if matches!(e, ast::Expr::Subquery(_)) && in_group_by.contains(e) {
                    self.shared_subqueries.push((e.clone(), None));
                }
                Ok(WalkControl::Continue)
            });
        }
    }

    fn bind_trigger_column(&self, expr: &mut ast::Expr) -> Result<bool> {
        let Some(bindings) = &self.trigger_columns else {
            return Ok(false);
        };
        let (namespace, column) = match expr {
            ast::Expr::Qualified(namespace, column)
            | ast::Expr::DoublyQualified(_, namespace, column) => (
                normalize_ident(namespace.as_str()),
                normalize_ident(column.as_str()),
            ),
            _ => return Ok(false),
        };

        let registers = if namespace.eq_ignore_ascii_case("new") {
            bindings.new_registers.as_deref().ok_or_else(|| {
                crate::LimboError::ParseError(
                    "NEW references are only valid in INSERT and UPDATE triggers".to_string(),
                )
            })?
        } else if namespace.eq_ignore_ascii_case("old") {
            bindings.old_registers.as_deref().ok_or_else(|| {
                crate::LimboError::ParseError(
                    "OLD references are only valid in UPDATE and DELETE triggers".to_string(),
                )
            })?
        } else {
            return Ok(false);
        };

        let register = if let Some((index, column_definition)) = bindings.table.get_column(&column)
        {
            if column_definition.is_rowid_alias() {
                registers.last().copied()
            } else {
                registers.get(index).copied()
            }
        } else if super::planner::ROWID_STRS
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&column))
        {
            registers.last().copied()
        } else {
            None
        };

        let Some(register) = register else {
            crate::bail_parse_error!("no such column in {}: {}", namespace.to_uppercase(), column);
        };
        *expr = ast::Expr::Register(register);
        Ok(true)
    }

    /// Bind an expression, resolving column references against the given scope.
    fn bind_expr(&mut self, expr: &mut ast::Expr, scope: &BindScope) -> Result<()> {
        self.bind_expr_with_expected_type(expr, scope, None)
    }

    fn bind_expr_with_expected_type(
        &mut self,
        expr: &mut ast::Expr,
        scope: &BindScope,
        expected_type: Option<&crate::schema::TypeDef>,
    ) -> Result<()> {
        walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
            match expr {
                ast::Expr::Between { .. } => {
                    // Keep BETWEEN's tested expression on the left side of
                    // both comparisons so SQLite collation precedence holds.
                    rewrite_between_node(expr);
                }
                ast::Expr::Id(_)
                | ast::Expr::Qualified(_, _)
                | ast::Expr::DoublyQualified(_, _, _) => {
                    self.bind_identifier(expr, scope)?;
                }
                ast::Expr::Exists(_) => {
                    let subquery_id = self.id_gen.next_table_id();
                    let ast::Expr::Exists(mut select) =
                        std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                    else {
                        unreachable!();
                    };
                    let inner_bound = self.bind_subquery_expr(&mut select, scope)?;
                    self.subquery_bindings.insert(
                        subquery_id,
                        BoundSubquery {
                            select,
                            inner_bound,
                        },
                    );
                    *expr = ast::Expr::SubqueryResult {
                        subquery_id,
                        lhs: None,
                        not_in: false,
                        query_type: ast::SubqueryType::Exists { result_reg: 0 },
                    };
                    return Ok(WalkControl::SkipChildren);
                }
                ast::Expr::Subquery(_) => {
                    // Scalar-subquery CSE: an occurrence shared between GROUP BY
                    // and the SELECT list reuses the first occurrence's id so
                    // both point at a single evaluation.
                    let shared_slot = self
                        .shared_subqueries
                        .iter()
                        .position(|(raw, _)| raw == expr);
                    if let Some(slot) = shared_slot {
                        if let (_, Some(existing_id)) = self.shared_subqueries[slot] {
                            let num_regs = self
                                .subquery_bindings
                                .get(&existing_id)
                                .map(|b| b.inner_bound.result_columns.len())
                                .unwrap_or(0);
                            *expr = ast::Expr::SubqueryResult {
                                subquery_id: existing_id,
                                lhs: None,
                                not_in: false,
                                query_type: ast::SubqueryType::RowValue {
                                    result_reg_start: 0,
                                    num_regs,
                                },
                            };
                            return Ok(WalkControl::SkipChildren);
                        }
                    }
                    let subquery_id = self.id_gen.next_table_id();
                    if let Some(slot) = shared_slot {
                        self.shared_subqueries[slot].1 = Some(subquery_id);
                    }
                    let ast::Expr::Subquery(mut select) =
                        std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                    else {
                        unreachable!();
                    };
                    let inner_bound = self.bind_subquery_expr(&mut select, scope)?;
                    let num_result_cols = inner_bound.result_columns.len();
                    self.subquery_bindings.insert(
                        subquery_id,
                        BoundSubquery {
                            select,
                            inner_bound,
                        },
                    );
                    *expr = ast::Expr::SubqueryResult {
                        subquery_id,
                        lhs: None,
                        not_in: false,
                        query_type: ast::SubqueryType::RowValue {
                            result_reg_start: 0,
                            num_regs: num_result_cols,
                        },
                    };
                    return Ok(WalkControl::SkipChildren);
                }
                ast::Expr::InSelect { .. } => {
                    let subquery_id = self.id_gen.next_table_id();
                    let ast::Expr::InSelect {
                        lhs,
                        not,
                        rhs: mut select,
                    } = std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                    else {
                        unreachable!();
                    };
                    // Bind lhs first against the current scope
                    // (already handled by walker for non-subquery children,
                    // but InSelect lhs needs explicit binding since we took ownership)
                    let mut lhs = lhs;
                    self.bind_expr(&mut lhs, scope)?;
                    let inner_bound = self.bind_subquery_expr(&mut select, scope)?;
                    self.subquery_bindings.insert(
                        subquery_id,
                        BoundSubquery {
                            select,
                            inner_bound,
                        },
                    );
                    *expr = ast::Expr::SubqueryResult {
                        subquery_id,
                        lhs: Some(lhs),
                        not_in: not,
                        query_type: ast::SubqueryType::In {
                            cursor_id: 0,
                            affinity_str: Arc::new(String::new()),
                        },
                    };
                    return Ok(WalkControl::SkipChildren);
                }
                // Validate struct/union/array function calls at bind time
                // (arity and literal-argument checks), mirroring
                // bind_and_rewrite_expr.
                ast::Expr::FunctionCall { name, args, .. } => {
                    super::expr::validate_custom_type_function_call(
                        name.as_str(),
                        args,
                        self.resolver,
                    )?;
                }
                ast::Expr::FunctionCallStar { .. } => {
                    self.expand_star_function(expr, scope);
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        })?;
        self.bind_custom_type_function_calls(expr, scope, expected_type)?;
        Ok(())
    }

    /// Expand `f(*)` for functions that need star expansion (json_object,
    /// jsonb_object) into alternating column-name / column-reference
    /// arguments over the scope's tables. Leaves the call untouched when the
    /// scope has no tables so translation can report the error.
    fn expand_star_function(&mut self, expr: &mut ast::Expr, scope: &BindScope) {
        let ast::Expr::FunctionCallStar { name, filter_over } = expr else {
            return;
        };
        let Ok(Some(func)) = Func::resolve_function(name.as_str(), 0) else {
            return;
        };
        if !func.needs_star_expansion() || scope.tables.is_empty() {
            return;
        }
        let mut args: Vec<Box<ast::Expr>> = Vec::new();
        for st in &scope.tables {
            for col_ref in st.table.columns() {
                if col_ref.is_hidden {
                    continue;
                }
                // Column name as string literal
                let quoted = format!("'{}'", col_ref.name);
                args.push(Box::new(ast::Expr::Literal(ast::Literal::String(quoted))));
                // Column reference
                args.push(Box::new(ast::Expr::Column {
                    database: None,
                    table: st.internal_id,
                    column: col_ref.idx,
                    is_rowid_alias: col_ref.is_rowid_alias,
                }));
                self.tracking.record_column(st.internal_id, col_ref.idx);
            }
        }
        *expr = ast::Expr::FunctionCall {
            name: name.clone(),
            distinctness: None,
            args,
            filter_over: filter_over.clone(),
            order_by: vec![],
            within_group: vec![],
        };
    }

