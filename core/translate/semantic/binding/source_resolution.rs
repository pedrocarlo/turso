    fn resolve_select_table(
        &mut self,
        table: &mut ast::SelectTable,
        lateral_scope: Option<&BindScope>,
        position: usize,
    ) -> Result<ScopeTable> {
        match table {
            // Named table: CTE lookup first, then schema lookup
            ast::SelectTable::Table(name, alias, indexed) => {
                let table_name = normalize_ident(name.name.as_str());
                // 1. Determine identifier (alias or table name)
                let identifier = alias
                    .as_ref()
                    .map(|a| normalize_ident(a.name().as_str()))
                    .unwrap_or_else(|| table_name.clone());

                // 2. Check self.ctes for a CTE match. Schema-qualified names
                // (e.g. main.t) always refer to schema objects, never CTEs.
                if let Some(cte) = self
                    .ctes
                    .get(&table_name)
                    .filter(|_| name.db_name.is_none())
                {
                    // Referencing a CTE whose body is currently being bound is
                    // a circular reference (identity-checked by cte_id, so a
                    // shadowing nested CTE with the same name is unaffected) —
                    // unless we are binding that CTE's own recursive arms, where
                    // the self-reference resolves to the recursive input table.
                    if self
                        .ctes_being_bound
                        .iter()
                        .any(|(id, _)| *id == cte.cte_id)
                    {
                        if let Some(recursive_self) = self
                            .recursive_self
                            .as_ref()
                            .filter(|recursive_self| recursive_self.cte_id == cte.cte_id)
                        {
                            return Ok(ScopeTable {
                                identifier,
                                internal_id: recursive_self.input_id,
                                source: ScopeTableSource::Cte { name: table_name },
                                table: recursive_self.table.clone(),
                                join_info: None,
                                database_id: 0,
                                indexed: None,
                                bound_index_method_patterns: Vec::new(),
                                bound_index_expressions: Vec::new(),
                            });
                        }
                        crate::bail_parse_error!("circular reference: {}", table_name);
                    }
                    // Surface any binding error deferred from the (lazy) CTE
                    // body bind.
                    if let Some(msg) = &cte.bind_error {
                        return Err(crate::LimboError::ParseError(msg.clone()));
                    }
                    validate_cte_explicit_columns(&table_name, cte)?;
                    //    - resolved_columns was populated by bind_cte pass 2
                    //    - Build Arc<CteTable> as the BindTable
                    let cte_table = Arc::new(CteTable {
                        columns: cte.resolved_columns.clone(),
                    });
                    // 4. Generate internal_id via self.id_gen.next_table_id()
                    return Ok(ScopeTable {
                        identifier,
                        internal_id: self.id_gen.next_table_id(),
                        source: ScopeTableSource::Cte { name: table_name },
                        table: cte_table,
                        join_info: None,
                        database_id: 0,
                        indexed: None,
                        bound_index_method_patterns: Vec::new(),
                        bound_index_expressions: Vec::new(),
                    });
                }

                // 3. Otherwise, schema lookup via resolver
                //    - Handle cross-database references (e.g. aux.t1)
                let database_id = self
                    .resolver
                    .resolve_existing_table_database_id_qualified(name)?;

                // 3a. Check for views — expand them as derived tables (subqueries)
                if let Some(view) = self
                    .resolver
                    .with_schema(database_id, |s| s.get_view(&table_name))
                {
                    view.process()?;
                    // Clone what we need before releasing the view reference.
                    // Keep Arc to original so we can call done() after binding.
                    let view_ref = view.clone(); // Arc clone, not View clone
                    let view_columns = view.columns.clone();
                    let mut view_select = view.select_stmt.clone();
                    // Apply view column aliases to the SELECT result columns
                    if let ast::OneSelect::Select {
                        ref mut columns, ..
                    } = view_select.body.select
                    {
                        for (col, result_col) in view_columns.iter().zip(columns.iter_mut()) {
                            if let (Some(name_str), ast::ResultColumn::Expr(_, ref mut col_alias)) =
                                (&col.name, result_col)
                            {
                                *col_alias = Some(ast::As::As(ast::Name::exact(name_str.clone())));
                            }
                        }
                    }

                    // Bind the view's SELECT as a derived table (subquery).
                    // Views resolve against the schema only — CTEs from the
                    // calling query must not leak into the view body.
                    let saved_ctes = std::mem::take(&mut self.ctes);
                    let bound_select = self.bind_select(&mut view_select);
                    self.ctes = saved_ctes;
                    // Reset view state so nested view-on-view chains don't
                    // falsely detect circular definitions during ALTER TABLE.
                    view_ref.done();
                    let bound_select = bound_select?;
                    let subquery_columns: Vec<String> = bound_select
                        .result_columns
                        .iter()
                        .map(|bc| bc.name.clone())
                        .collect();
                    let subquery_table = Arc::new(DerivedTable {
                        columns: subquery_columns,
                    });

                    let internal_id = self.id_gen.next_table_id();

                    self.derived_bindings.insert(
                        internal_id,
                        BoundSubquery {
                            select: view_select,
                            inner_bound: bound_select,
                        },
                    );

                    return Ok(ScopeTable {
                        identifier,
                        internal_id,
                        source: ScopeTableSource::Derived {},
                        table: subquery_table,
                        join_info: None,
                        database_id: 0,
                        indexed: None,
                        bound_index_method_patterns: Vec::new(),
                        bound_index_expressions: Vec::new(),
                    });
                }

                // 3b. Materialized views with storage are treated as
                // regular BTree tables.
                let matview = self.resolver.with_schema(database_id, |schema| {
                    schema.get_materialized_view(&table_name)
                });
                if let Some(view) = matview {
                    let has_compatible_state = self.resolver.with_schema(database_id, |schema| {
                        schema.has_compatible_dbsp_state_table(&table_name)
                    });
                    if !has_compatible_state {
                        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
                        return Err(crate::LimboError::InternalError(format!(
                            "Materialized view '{table_name}' has an incompatible version. \n\
                             The current version is {DBSP_CIRCUIT_VERSION}, but the view was created with a different version. \n\
                             Please DROP and recreate the view to use it."
                        )));
                    }
                    let view_guard = view.lock();
                    let root_page = view_guard.get_root_page();
                    if root_page == 0 {
                        drop(view_guard);
                        return Err(crate::LimboError::InternalError(
                            "Materialized view has no storage allocated".to_string(),
                        ));
                    }
                    let columns = view_guard.column_schema.flat_columns();
                    let btree_table = Arc::new(crate::schema::BTreeTable::new(
                        root_page,
                        view_guard.name().to_string(),
                        crate::alloc::vec![],
                        columns,
                        crate::schema::BTreeCharacteristics::HAS_ROWID,
                        crate::alloc::vec![],
                        crate::alloc::vec![],
                        crate::alloc::vec![],
                        None,
                    ));
                    drop(view_guard);
                    let table = Arc::new(Table::BTree(btree_table));
                    return Ok(ScopeTable {
                        identifier,
                        internal_id: self.id_gen.next_table_id(),
                        source: ScopeTableSource::Table(table.clone()),
                        table,
                        join_info: None,
                        database_id,
                        indexed: None,
                        bound_index_method_patterns: Vec::new(),
                        bound_index_expressions: Vec::new(),
                    });
                }

                // 3c. Regular table lookup
                let Some(schema_table) = self
                    .resolver
                    .with_schema(database_id, |s| s.get_table(&table_name))
                else {
                    // Incompatible materialized view?
                    let is_incompatible = self.resolver.with_schema(database_id, |schema| {
                        schema.incompatible_views.contains(&table_name)
                    });
                    if is_incompatible {
                        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
                        crate::bail_parse_error!(
                            "Materialized view '{}' has an incompatible version. \n\
                             The view was created with a different DBSP version than the current version ({}). \n\
                             Please DROP and recreate the view to use it.",
                            table_name,
                            DBSP_CIRCUIT_VERSION
                        );
                    }
                    // A view row whose stored SQL failed to parse at schema load
                    let is_broken_view = self.resolver.with_schema(database_id, |schema| {
                        schema.broken_views.contains(&table_name)
                    });
                    if is_broken_view {
                        crate::bail_parse_error!(
                            "view '{}' could not be loaded: its SQL in sqlite_schema does not parse. \n\
                             Use DROP VIEW to remove it, then recreate it.",
                            table_name
                        );
                    }
                    crate::bail_parse_error!("no such table: {table_name}");
                };

                // 4. Generate internal_id via self.id_gen.next_table_id()
                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Table(schema_table.clone()),
                    table: schema_table,
                    join_info: None,
                    database_id,
                    indexed: indexed.clone(),
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                })
            }
            // Inline subquery in FROM: SELECT ... FROM (SELECT ...)
            ast::SelectTable::Select(subselect, alias) => {
                let identifier = alias
                    .as_ref()
                    .map(|a| normalize_ident(a.name().as_str()))
                    .unwrap_or_else(|| format!("(subquery-{position})"));

                // FROM subqueries don't correlate with the query being built.
                let bound_select = self.bind_select(subselect)?;

                let subquery_columns: Vec<String> = bound_select
                    .result_columns
                    .iter()
                    .map(|bc| bc.name.clone())
                    .collect();
                let subquery_table = Arc::new(DerivedTable {
                    columns: subquery_columns,
                });

                let internal_id = self.id_gen.next_table_id();

                // Store the binding for planning before into_table_references.
                self.derived_bindings.insert(
                    internal_id,
                    BoundSubquery {
                        select: subselect.clone(),
                        inner_bound: bound_select,
                    },
                );

                Ok(ScopeTable {
                    identifier,
                    internal_id,
                    source: ScopeTableSource::Derived {},
                    table: subquery_table,
                    join_info: None,
                    database_id: 0,
                    indexed: None,
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                })
            }
            // Virtual table function call: SELECT ... FROM table_func(args)
            ast::SelectTable::TableCall(name, args, alias) => {
                let table_name = normalize_ident(name.name.as_str());
                // Call arguments on a CTE are an error.
                if name.db_name.is_none() && self.ctes.contains_key(&table_name) && !args.is_empty()
                {
                    // A recursive self-reference gets SQLite's table-valued
                    // function message; a plain CTE reference gets the
                    // not-a-function message.
                    let is_recursive_self = self
                        .recursive_self
                        .as_ref()
                        .zip(self.ctes.get(&table_name))
                        .is_some_and(|(recursive_self, cte)| recursive_self.cte_id == cte.cte_id);
                    if is_recursive_self {
                        crate::bail_parse_error!(
                            "too many arguments on {}() - max 0",
                            name.name.as_str()
                        );
                    }
                    crate::bail_parse_error!("'{}' is not a function", name.name.as_str());
                }
                // 1. Look up the virtual table via resolver
                let schema_table =
                    self.resolver
                        .schema()
                        .get_table(&table_name)
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!("no such table: {table_name}"))
                        })?;
                // Call arguments on a plain table are an error too — only
                // virtual tables (table-valued functions) accept them.
                if !args.is_empty() && schema_table.btree().is_some() {
                    crate::bail_parse_error!("'{}' is not a function", name.name.as_str());
                }

                let identifier = alias
                    .as_ref()
                    .map(|a| normalize_ident(a.name().as_str()))
                    .unwrap_or_else(|| table_name.clone());

                // 2. Bind argument expressions. Use lateral scope if available so that
                // table function args can reference previously-joined tables
                // (e.g. SELECT * FROM generate_series(0,2) s JOIN json_tree(..., s.value)).
                // Use allow_unbound so that forward references to later FROM tables
                // (e.g. FROM func(s.col), s) are left unresolved for the emitter.
                let empty_scope = BindScope::empty();
                let arg_scope = lateral_scope.unwrap_or(&empty_scope);
                let saved_allow_unbound = self.allow_unbound;
                self.allow_unbound = true;
                for arg in args.iter_mut() {
                    self.bind_expr(arg, arg_scope)?;
                }
                self.allow_unbound = saved_allow_unbound;

                // 3. Build ScopeTable from the virtual table's columns
                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Table(schema_table.clone()),
                    table: schema_table,
                    join_info: None,
                    database_id: 0, // Virtual tables are always in main schema
                    indexed: None,
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                })
            }
            // Parenthesized FROM subclause: SELECT ... FROM (t1 JOIN t2 ON ...)
            ast::SelectTable::Sub(from_clause, alias) => {
                // 1. Recursively bind_from(from_clause)
                let inner_scope = self.bind_from(from_clause)?;

                // 2-3. Collect all column names from inner scope tables
                let all_columns: Vec<String> = inner_scope
                    .tables
                    .iter()
                    .flat_map(|table| table.table.columns())
                    .map(|col| col.name.to_string())
                    .collect();

                let identifier = alias
                    .as_ref()
                    .map(|a| normalize_ident(a.name().as_str()))
                    .unwrap_or_else(|| format!("(subquery-{position})"));

                // If alias is present, wrap all columns under that alias
                // If no alias, flatten tables into parent scope
                let sub_table = Arc::new(DerivedTable {
                    columns: all_columns,
                });

                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Derived {},
                    table: sub_table,
                    join_info: None,
                    database_id: 0,
                    indexed: None,
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                })
            }
        }
    }

