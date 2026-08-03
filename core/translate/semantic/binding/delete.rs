    // ── DELETE binding ──────────────────────────────────────────────────

    /// Bind a DELETE statement, resolving all name references in-place.
    pub fn bind_delete(
        &mut self,
        tbl_name: &ast::QualifiedName,
        indexed: Option<ast::Indexed>,
        where_clause: &mut Option<Box<ast::Expr>>,
        returning: &mut Vec<ast::ResultColumn>,
        with: &mut Option<ast::With>,
        database_id: usize,
        table: Arc<Table>,
    ) -> Result<BoundDelete> {
        self.with_query(|ctx| {
            // 1. Bind CTEs from WITH clause
            if let Some(with) = with.as_mut() {
                ctx.bind_cte(with)?;
            }

            // 2. Build scope with target table
            let scope = ctx.build_table_scope(
                &tbl_name.name,
                tbl_name.alias.as_ref(),
                indexed,
                database_id,
            )?;

            // 3. Bind WHERE clause
            if let Some(where_expr) = where_clause.as_mut() {
                ctx.with_phase(BindPhase::NoAliases, |ctx| {
                    ctx.bind_expr(where_expr, &scope)
                })?;
            }

            // 4. Bind RETURNING (expand stars, bind exprs)
            ctx.bind_returning(returning, &scope)?;

            // 5. Extract CTE definitions in definition order
            let cte_definitions: Vec<(String, CteEntry)> = if let Some(with) = with.as_ref() {
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

            Ok(BoundDelete {
                scope,
                tracking: std::mem::take(&mut ctx.tracking),
                subquery_bindings: std::mem::take(&mut ctx.subquery_bindings),
                cte_definitions,
                database_id,
                table,
            })
        })
    }

