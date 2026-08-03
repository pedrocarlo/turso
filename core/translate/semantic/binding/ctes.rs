    fn bind_cte(&mut self, with: &mut ast::With) -> Result<()> {
        // Collect CTE names in definition order for referenced_cte_indices lookup.
        let mut cte_names: Vec<String> = Vec::with_capacity(with.ctes.len());
        let mut referenced_tables_by_cte: Vec<Vec<String>> = Vec::with_capacity(with.ctes.len());

        // Pass 1: register all CTE names and allocate IDs. Bodies are not
        // bound yet — SQLite resolves a CTE body only when the CTE is
        // referenced, so binding errors are deferred via `bind_error`.
        for cte in &with.ctes {
            let cte_name = normalize_ident(cte.tbl_name.as_str());
            // Check for duplicates within the same WITH clause only.
            // Inner WITH clauses are allowed to shadow outer CTE names.
            if cte_names.contains(&cte_name) {
                crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
            }
            let explicit_columns: Vec<String> = cte
                .columns
                .iter()
                .map(|c| normalize_ident(c.col_name.as_str()))
                .collect();

            let cte_id = self.id_gen.next_cte_id();
            let materialize_hint = cte.materialized == turso_parser::ast::Materialized::Yes;

            // Table names this body references (schema-qualified names are
            // excluded — they can never refer to a CTE). Sibling dependency
            // edges are computed from these once all names are known.
            let mut referenced_tables = Vec::new();
            crate::translate::planner::collect_from_clause_table_refs(
                &cte.select,
                &mut referenced_tables,
            );

            // A body that references its own name is a recursive CTE (the
            // RECURSIVE keyword is not required, matching SQLite) — unless the
            // reference is in the first arm, which is a circular reference.
            let (recursive, first_arm_self_ref) =
                crate::translate::planner::cte_self_reference_info(&cte_name, &cte.select);
            let bind_error = first_arm_self_ref
                .then(|| format!("circular reference: {}", cte.tbl_name.as_str()));

            referenced_tables_by_cte.push(referenced_tables);
            cte_names.push(cte_name.clone());
            self.insert_cte(
                cte_name,
                CteEntry {
                    select: cte.select.clone(),
                    // Explicit columns are the reference-visible columns and
                    // are known up front — forward references to this CTE can
                    // bind before its body does.
                    resolved_columns: explicit_columns.clone(),
                    explicit_columns,
                    cte_id,
                    result_column_count: 0,
                    inner_bound: None,
                    referenced_cte_indices: SmallVec::new(),
                    materialize_hint,
                    recursive,
                    recursive_binding: None,
                    bind_error,
                },
            );
        }

        // Sibling dependency edges. Forward references are included: SQLite
        // allows a CTE to reference one defined later in the same WITH clause.
        for (idx, referenced_tables) in referenced_tables_by_cte.iter().enumerate() {
            let indices: SmallVec<[usize; 2]> = (0..cte_names.len())
                .filter(|&i| i != idx && referenced_tables.contains(&cte_names[i]))
                .collect();
            self.ctes
                .get_mut(&cte_names[idx])
                .unwrap()
                .referenced_cte_indices = indices;
        }

        // Pass 2: bind bodies dependency-first so referenced siblings (in
        // either direction) have their result columns resolved before any
        // body that reads them. We collect inner_bound values separately
        // because bind_select calls with_query which clones self.ctes
        // (setting inner_bound = None via the custom Clone impl), then
        // restores them — destroying inner_bound values set in prior
        // iterations.
        let mut inner_bounds: Vec<(String, BoundSelect)> = Vec::with_capacity(with.ctes.len());
        let mut recursive_bindings: Vec<(String, RecursiveCteBinding)> = Vec::new();
        let mut done = vec![false; with.ctes.len()];
        for idx in 0..with.ctes.len() {
            self.bind_one_cte(
                with,
                &cte_names,
                idx,
                &mut done,
                &mut inner_bounds,
                &mut recursive_bindings,
            )?;
        }
        // Assign inner_bound values after all binding is done.
        for (cte_name, bound) in inner_bounds {
            self.ctes.get_mut(&cte_name).unwrap().inner_bound = Some(bound);
        }
        for (cte_name, binding) in recursive_bindings {
            self.ctes.get_mut(&cte_name).unwrap().recursive_binding = Some(binding);
        }
        Ok(())
    }

    /// Bind one CTE body (dependencies first). Errors don't propagate: they
    /// are stored on the entry and surface when the CTE is referenced,
    /// matching SQLite's lazy resolution of CTE bodies.
    fn bind_one_cte(
        &mut self,
        with: &mut ast::With,
        cte_names: &[String],
        idx: usize,
        done: &mut [bool],
        inner_bounds: &mut Vec<(String, BoundSelect)>,
        recursive_bindings: &mut Vec<(String, RecursiveCteBinding)>,
    ) -> Result<()> {
        if done[idx] {
            return Ok(());
        }
        done[idx] = true;
        let cte_name = cte_names[idx].clone();
        let Some(entry) = self.ctes.get(&cte_name) else {
            return Ok(());
        };
        if entry.bind_error.is_some() {
            return Ok(());
        }
        let cte_id = entry.cte_id;
        let deps = entry.referenced_cte_indices.clone();
        let is_recursive = entry.recursive;

        self.ctes_being_bound.push((cte_id, cte_name.clone()));
        let result: Result<()> = (|| {
            for &dep in &deps {
                let dep_id = self.ctes.get(&cte_names[dep]).map(|e| e.cte_id);
                if dep_id.is_some_and(|id| self.ctes_being_bound.iter().any(|(b, _)| *b == id)) {
                    crate::bail_parse_error!("circular reference: {}", cte_names[dep]);
                }
                self.bind_one_cte(with, cte_names, dep, done, inner_bounds, recursive_bindings)?;
            }
            let cte = &mut with.ctes[idx];
            if is_recursive {
                // Structure errors (circular reference, multiple recursive
                // references) match the recursive planner's exactly.
                let first_recursive_idx =
                    crate::translate::planner::validate_recursive_cte_structure(
                        &cte_name,
                        &cte.select,
                    )?;
                // Bind the initial (non-recursive) arms as one SELECT,
                // including the body-level WITH. SQLite takes the CTE's
                // column names and arity from the left-most arm, which
                // cannot reference the recursive table.
                let mut initial = ast::Select {
                    with: cte.select.with.clone(),
                    body: ast::SelectBody {
                        select: cte.select.body.select.clone(),
                        compounds: cte.select.body.compounds[..first_recursive_idx - 1].to_vec(),
                    },
                    order_by: vec![],
                    limit: None,
                };
                let initial_bound = self.bind_select(&mut initial)?;
                let entry = self.ctes.get_mut(&cte_name).unwrap();
                entry.result_column_count = initial_bound.result_columns.len();
                if entry.explicit_columns.is_empty() {
                    entry.resolved_columns = initial_bound
                        .result_columns
                        .iter()
                        .map(|bc| bc.name.clone())
                        .collect();
                }
                let self_table = Arc::new(CteTable {
                    columns: entry.resolved_columns.clone(),
                });

                // Bind each recursive arm as its own single-arm SELECT with
                // the CTE's own name resolving to the recursive input table.
                // Every self-reference shares input_id.
                let input_id = self.id_gen.next_table_id();
                let saved_self = self.recursive_self.replace(RecursiveSelfRef {
                    cte_id,
                    input_id,
                    table: self_table,
                });
                let arms_result = (|| -> Result<Vec<BoundSubquery>> {
                    let cte = &with.ctes[idx];
                    let mut arms = Vec::with_capacity(
                        cte.select.body.compounds.len() - (first_recursive_idx - 1),
                    );
                    for compound in &cte.select.body.compounds[first_recursive_idx - 1..] {
                        let mut arm = ast::Select {
                            with: cte.select.with.clone(),
                            body: ast::SelectBody {
                                select: compound.select.clone(),
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        };
                        let inner_bound = self.bind_select(&mut arm)?;
                        arms.push(BoundSubquery {
                            select: arm,
                            inner_bound,
                        });
                    }
                    Ok(arms)
                })();
                self.recursive_self = saved_self;
                let recursive_arms = arms_result?;
                let last_recursive_count = recursive_arms
                    .last()
                    .expect("recursive CTE must have a recursive arm")
                    .inner_bound
                    .result_columns
                    .len();
                if recursive_arms[..recursive_arms.len() - 1]
                    .iter()
                    .any(|arm| arm.inner_bound.result_columns.len() != last_recursive_count)
                {
                    crate::bail_parse_error!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        ast::CompoundOperator::UnionAll
                    );
                }
                let recursive_operator =
                    with.ctes[idx].select.body.compounds[first_recursive_idx - 1].operator;
                if initial_bound.result_columns.len() != last_recursive_count {
                    crate::bail_parse_error!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        recursive_operator
                    );
                }

                let mut result_column_arms: Vec<&[BoundColumn]> = Vec::new();
                result_column_arms.push(initial_bound.result_columns.as_slice());
                result_column_arms.extend(
                    initial_bound
                        .compound_result_columns
                        .iter()
                        .map(|columns| columns.as_slice()),
                );
                result_column_arms.extend(
                    recursive_arms
                        .iter()
                        .map(|arm| arm.inner_bound.result_columns.as_slice()),
                );
                let queue_order = resolve_compound_order_by(
                    &self.ctes.get(&cte_name).unwrap().select.order_by,
                    &result_column_arms,
                )?;
                // Bind the body-level LIMIT/OFFSET up front. They are
                // scope-less (identifiers cannot resolve against the body's
                // tables), and the recursive-CTE planner consumes them as
                // already bound.
                let resolver = self.resolver;
                let mut limit = {
                    let entry = self.ctes.get_mut(&cte_name).unwrap();
                    entry.select.order_by.clear();
                    entry.select.limit.take()
                };
                if let Some(limit) = limit.as_mut() {
                    let empty_scope = BindScope::empty();
                    bind_scopeless_expr(&mut limit.expr, resolver)?;
                    self.bind_custom_type_function_calls(&mut limit.expr, &empty_scope, None)?;
                    if let Some(offset) = limit.offset.as_mut() {
                        bind_scopeless_expr(offset, resolver)?;
                        self.bind_custom_type_function_calls(offset, &empty_scope, None)?;
                    }
                }
                self.ctes.get_mut(&cte_name).unwrap().select.limit = limit;
                recursive_bindings.push((
                    cte_name.clone(),
                    RecursiveCteBinding {
                        initial: BoundSubquery {
                            select: initial,
                            inner_bound: initial_bound,
                        },
                        recursive_arms,
                        first_recursive_arm_index: first_recursive_idx,
                        input_id,
                        queue_order,
                    },
                ));
            } else {
                let bound = self.bind_select(&mut cte.select)?;
                let entry = self.ctes.get_mut(&cte_name).unwrap();
                entry.result_column_count = bound.result_columns.len();
                if entry.explicit_columns.is_empty() {
                    entry.resolved_columns = bound
                        .result_columns
                        .iter()
                        .map(|bc| bc.name.clone())
                        .collect();
                }
                entry.select = cte.select.clone();
                inner_bounds.push((cte_name.clone(), bound));
            }
            Ok(())
        })();
        self.ctes_being_bound.pop();
        if let Err(err) = result {
            let msg = match err {
                crate::LimboError::ParseError(m) => m,
                other => other.to_string(),
            };
            if let Some(entry) = self.ctes.get_mut(&cte_name) {
                entry.bind_error = Some(msg);
            }
        }
        Ok(())
    }

