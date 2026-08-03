    // ── INSERT binding ──────────────────────────────────────────────────

    /// Bind an INSERT statement's RETURNING clause (with its WITH-clause CTEs
    /// for subquery resolution). The caller supplies the internal id it
    /// already allocated for the target table so the bound column references
    /// line up with its `TableReferences`.
    #[allow(clippy::type_complexity)]
    fn bind_insert_returning(
        &mut self,
        tbl_name: &ast::Name,
        target_internal_id: TableInternalId,
        returning: &mut Vec<ast::ResultColumn>,
        with: &mut Option<ast::With>,
        database_id: usize,
    ) -> Result<(
        Vec<(String, CteEntry)>,
        HashMap<ast::TableInternalId, BoundSubquery>,
    )> {
        self.with_query(|ctx| {
            // 1. Bind CTEs from the WITH clause
            if let Some(with) = with.as_mut() {
                ctx.bind_cte(with)?;
            }

            // 2. Build the target scope with the caller-provided id.
            // RETURNING always resolves through the schema table name.
            let normalized = normalize_ident(tbl_name.as_str());
            let schema_table = ctx
                .resolver
                .with_schema(database_id, |s| s.get_table(&normalized))
                .ok_or_else(|| {
                    crate::LimboError::ParseError(format!("no such table: {normalized}"))
                })?;
            let scope = BindScope {
                tables: vec![ScopeTable {
                    identifier: normalized,
                    internal_id: target_internal_id,
                    source: ScopeTableSource::Table(schema_table.clone()),
                    table: schema_table,
                    join_info: None,
                    database_id,
                    indexed: None,
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                }],
                right_join_swapped: false,
            };

            // 3. Bind RETURNING (expand stars, bind exprs)
            ctx.bind_returning(returning, &scope)?;

            // 4. Extract CTE definitions in definition order
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

            Ok((cte_definitions, std::mem::take(&mut ctx.subquery_bindings)))
        })
    }

    /// Infer a column name from an expression (for RETURNING without alias).
    fn infer_column_name(expr: &ast::Expr) -> String {
        match expr {
            ast::Expr::Column {
                database: _,
                table: _,
                column: _,
                ..
            } => {
                // After binding, we can't easily recover the original name.
                // The caller should use the alias or fall back to the original text.
                String::new()
            }
            ast::Expr::Id(name) => name.as_str().to_string(),
            ast::Expr::Qualified(_, name) => name.as_str().to_string(),
            _ => String::new(),
        }
    }
}

