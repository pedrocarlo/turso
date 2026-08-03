struct OuterQueryFrame {
    scope: BindScopeRef,
    aliases: Arc<Vec<BoundColumn>>,
}

impl BoundSelect {
    /// Convert the bound scopes into `TableReferences` (one per SELECT core,
    /// main scope first). Outer query references are set on each scope's
    /// `TableReferences` so that column usage tracking for correlated columns
    /// can find their target tables.
    pub fn into_table_references_with_outer_refs(
        self,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
        planned_derived: &mut HashMap<ast::TableInternalId, super::plan::JoinedTable>,
        outer_query_refs: Vec<super::plan::OuterQueryReference>,
    ) -> Result<Vec<TableReferences>> {
        let mut all = Vec::with_capacity(1 + self.compound_scopes.len());

        // Compound scopes get the same outer refs as the main scope: a
        // correlated compound subquery (e.g. `x IN (... UNION ...)`) can
        // reference the outer scope from any of its constituent SELECTs.
        for scope in self.compound_scopes {
            all.push(Self::scope_to_table_references(
                scope,
                &self.tracking,
                planned_ctes,
                planned_derived,
                outer_query_refs.clone(),
            )?);
        }
        let main_refs = Self::scope_to_table_references(
            self.main_scope,
            &self.tracking,
            planned_ctes,
            planned_derived,
            outer_query_refs,
        )?;
        all.insert(0, main_refs);

        Ok(all)
    }

    fn scope_to_table_references(
        scope: BindScope,
        tracking: &BindTracking,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
        planned_derived: &mut HashMap<ast::TableInternalId, super::plan::JoinedTable>,
        outer_query_refs: Vec<super::plan::OuterQueryReference>,
    ) -> Result<TableReferences> {
        let right_join_swapped = scope.right_join_swapped;
        let joined_tables = scope
            .tables
            .into_iter()
            .map(|scope_table| match scope_table.source {
                ScopeTableSource::Table(table) => Ok(super::plan::JoinedTable {
                    op: super::plan::Operation::default_scan_for(&table),
                    column_use_counts: vec![0; table.columns().len()],
                    table: (*table).clone(),
                    identifier: scope_table.identifier,
                    internal_id: scope_table.internal_id,
                    join_info: scope_table.join_info,
                    col_used_mask: Default::default(),
                    expression_index_usages: Vec::new(),
                    database_id: scope_table.database_id,
                    indexed: scope_table.indexed,
                    bound_index_method_patterns: scope_table.bound_index_method_patterns,
                    bound_index_expressions: scope_table.bound_index_expressions,
                }),
                ScopeTableSource::Cte { name, .. } => {
                    // Clone rather than remove: the same CTE may be referenced
                    // multiple times (e.g. FROM cte t1 JOIN cte t2).
                    let mut cte_table = planned_ctes.get(&name).cloned().ok_or_else(|| {
                        crate::LimboError::InternalError(format!(
                            "CTE '{name}' was not planned before into_table_references"
                        ))
                    })?;
                    cte_table.identifier = scope_table.identifier;
                    cte_table.internal_id = scope_table.internal_id;
                    cte_table.join_info = scope_table.join_info;
                    // CTE's FromClauseSubquery.name is already set to the CTE
                    // definition name during plan_bound_ctes — don't overwrite
                    // with the alias.
                    Ok(cte_table)
                }
                ScopeTableSource::Derived { .. } => {
                    let mut derived_table = planned_derived
                        .remove(&scope_table.internal_id)
                        .ok_or_else(|| {
                            crate::LimboError::InternalError(format!(
                                "derived table '{}' was not planned before into_table_references",
                                scope_table.identifier
                            ))
                        })?;
                    derived_table.identifier = scope_table.identifier.clone();
                    derived_table.internal_id = scope_table.internal_id;
                    derived_table.join_info = scope_table.join_info;
                    // Also update the inner FromClauseSubquery name so that
                    // index_seek_affinities can match the table by name.
                    if let Table::FromClauseSubquery(subq) = &mut derived_table.table {
                        Arc::make_mut(subq).name = scope_table.identifier;
                    }
                    Ok(derived_table)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let mut table_references = TableReferences::new(joined_tables, outer_query_refs);
        if right_join_swapped {
            // The planner's select_star consults this to restore the original
            // column order when RIGHT JOIN planning swapped the tables.
            table_references.set_right_join_swapped();
        }
        tracking.flush(&mut table_references);
        Ok(table_references)
    }
}

