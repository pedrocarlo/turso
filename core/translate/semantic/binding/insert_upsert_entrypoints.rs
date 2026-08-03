pub enum BoundUpsertDo {
    Nothing,
    Update {
        sets: Vec<(usize, Box<ast::Expr>)>,
        where_clause: Option<Box<ast::Expr>>,
    },
}

pub type BoundUpsertAction = (
    super::upsert::ResolvedUpsertTarget,
    crate::vdbe::BranchOffset,
    BoundUpsertDo,
);

/// Output of the statement-level bind phase for INSERT.
///
/// Single-row VALUES bind scope-less because they have no FROM clause.
/// UPSERT conflict targets bind against the schema table and `DO UPDATE`
/// expressions bind against the target row plus the EXCLUDED pseudo-table.
/// Multi-row/SELECT sources are fully bound here. Virtual table inserts still
/// consume their restricted VALUES body directly.
pub struct BoundInsert {
    #[allow(clippy::vec_box)]
    pub values: Vec<Box<ast::Expr>>,
    pub upsert_actions: Vec<BoundUpsertAction>,
    pub inserting_multiple_rows: bool,
    /// Database the target table lives in (0 = main).
    pub database_id: usize,
    /// The validated target table.
    pub table: Arc<Table>,
    /// Bound source for INSERT SELECT and VALUES paths that use a coroutine.
    pub source_select: Option<BoundSelect>,
    /// WITH-clause definitions visible to RETURNING subqueries.
    pub returning_cte_definitions: Vec<(String, CteEntry)>,
    /// Subqueries extracted while binding RETURNING expressions.
    pub returning_subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    /// ID used by bound references to the current target row.
    pub target_table_id: ast::TableInternalId,
    /// ID used by bound references to the would-be inserted row.
    pub excluded_table_id: ast::TableInternalId,
    /// Stored index keys and predicates bound to the target table reference.
    pub bound_index_expressions: Vec<super::plan::BoundIndexExpressions>,
}

fn insert_value_types(
    table: &Table,
    columns: &[ast::Name],
    resolver: &Resolver,
) -> Result<Vec<Option<Arc<TypeDef>>>> {
    if columns.is_empty() {
        return Ok(table
            .columns()
            .iter()
            .filter(|column| !column.hidden() && !column.is_generated())
            .map(|column| {
                resolver
                    .schema()
                    .get_type_def_unchecked(&column.ty_str)
                    .cloned()
            })
            .collect());
    }

    columns
        .iter()
        .map(|name| {
            let name = normalize_ident(name.as_str());
            if let Some((_, column)) = table.get_column_by_name(&name) {
                column.ensure_not_generated("INSERT into", &name)?;
                Ok(resolver
                    .schema()
                    .get_type_def_unchecked(&column.ty_str)
                    .cloned())
            } else if super::planner::ROWID_STRS
                .iter()
                .any(|rowid| rowid.eq_ignore_ascii_case(&name))
            {
                Ok(None)
            } else {
                crate::bail_parse_error!("table {} has no column named {}", table.get_name(), name)
            }
        })
        .collect()
}

fn upsert_scope_table(
    identifier: String,
    internal_id: ast::TableInternalId,
    database_id: usize,
    table: &Arc<Table>,
) -> ScopeTable {
    ScopeTable {
        identifier,
        internal_id,
        source: ScopeTableSource::Table(Arc::clone(table)),
        table: Arc::clone(table) as Arc<dyn BindTable>,
        join_info: None,
        database_id,
        indexed: None,
        bound_index_method_patterns: Vec::new(),
        bound_index_expressions: Vec::new(),
    }
}

fn bind_upsert_conflict_target<G: IdGenerator>(
    binder: &mut BindContext<'_, G>,
    upsert: &mut ast::Upsert,
    database_id: usize,
    table: &Arc<Table>,
) -> Result<()> {
    let Some(target) = upsert.index.as_mut() else {
        return Ok(());
    };
    let scope = BindScope {
        tables: vec![upsert_scope_table(
            normalize_ident(table.get_name()),
            ast::TableInternalId::SELF_TABLE,
            database_id,
            table,
        )],
        right_join_swapped: false,
    };
    binder.with_phase(BindPhase::NoAliases, |binder| {
        for target in &mut target.targets {
            binder.bind_expr(&mut target.expr, &scope)?;
        }
        if let Some(where_clause) = target.where_clause.as_mut() {
            binder.bind_expr(where_clause, &scope)?;
        }
        Ok(())
    })?;

    // Schema expressions use SELF_TABLE without a database qualifier. Keep
    // conflict targets in that same canonical form before comparing them.
    walk_expr_mut_in_upsert_target(target, &mut |expr| {
        match expr {
            ast::Expr::Column {
                database, table, ..
            }
            | ast::Expr::RowId { database, table }
                if table.is_self_table() =>
            {
                *database = None;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn walk_expr_mut_in_upsert_target(
    target: &mut ast::UpsertIndex,
    visitor: &mut impl FnMut(&mut ast::Expr) -> Result<WalkControl>,
) -> Result<()> {
    for column in &mut target.targets {
        walk_expr_mut(&mut column.expr, visitor)?;
    }
    if let Some(where_clause) = target.where_clause.as_mut() {
        walk_expr_mut(where_clause, visitor)?;
    }
    Ok(())
}

fn collect_upsert_set_clauses(
    table: &Table,
    set_items: Vec<ast::Set>,
) -> Result<Vec<(usize, Box<ast::Expr>)>> {
    let lookup: HashMap<String, usize> = table
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            column
                .name
                .as_ref()
                .map(|name| (name.to_lowercase(), index))
        })
        .collect();
    let mut result = Vec::new();

    for set in set_items {
        let values = match *set.expr {
            ast::Expr::Parenthesized(values) => values,
            expr => vec![Box::new(expr)],
        };
        if set.col_names.len() != values.len() {
            crate::bail_parse_error!(
                "{} columns assigned {} values",
                set.col_names.len(),
                values.len()
            );
        }
        for (name, expr) in set.col_names.iter().zip(values) {
            let Some(index) = lookup.get(&normalize_ident(name.as_str())).copied() else {
                crate::bail_parse_error!("no such column: {}", name);
            };
            table.columns()[index].ensure_not_generated("UPDATE", name.as_str())?;
            if let Some(existing) = result
                .iter_mut()
                .find(|(existing_index, _)| *existing_index == index)
            {
                existing.1 = expr;
            } else {
                result.push((index, expr));
            }
        }
    }
    Ok(result)
}

fn assignment_type(
    table: &Table,
    column_name: &ast::Name,
    resolver: &Resolver,
) -> Result<Option<Arc<TypeDef>>> {
    let name = normalize_ident(column_name.as_str());
    let Some((_, column)) = table.get_column_by_name(&name) else {
        crate::bail_parse_error!("no such column: {}", column_name);
    };
    Ok(resolver
        .schema()
        .get_type_def_unchecked(&column.ty_str)
        .cloned())
}

fn bind_update_set<G: IdGenerator>(
    binder: &mut BindContext<'_, G>,
    set: &mut ast::Set,
    scope: &BindScope,
    table: &Table,
) -> Result<()> {
    match set.expr.as_mut() {
        ast::Expr::Parenthesized(values) => {
            if set.col_names.len() != values.len() {
                crate::bail_parse_error!(
                    "{} columns assigned {} values",
                    set.col_names.len(),
                    values.len()
                );
            }
            for (column_name, expr) in set.col_names.iter().zip(values) {
                let expected_type = assignment_type(table, column_name, binder.resolver)?;
                binder.bind_expr_with_expected_type(expr, scope, expected_type.as_deref())?;
            }
        }
        expr => {
            if set.col_names.len() != 1 {
                crate::bail_parse_error!("{} columns assigned 1 values", set.col_names.len());
            }
            let expected_type = assignment_type(table, &set.col_names[0], binder.resolver)?;
            binder.bind_expr_with_expected_type(expr, scope, expected_type.as_deref())?;
        }
    }
    Ok(())
}

fn bind_upsert_do<G: IdGenerator>(
    binder: &mut BindContext<'_, G>,
    do_clause: ast::UpsertDo,
    target_identifier: String,
    target_table_id: ast::TableInternalId,
    excluded_table_id: ast::TableInternalId,
    database_id: usize,
    table: &Arc<Table>,
) -> Result<BoundUpsertDo> {
    let ast::UpsertDo::Set {
        sets,
        mut where_clause,
    } = do_clause
    else {
        return Ok(BoundUpsertDo::Nothing);
    };
    let mut sets = collect_upsert_set_clauses(table, sets)?;
    let target_scope = BindScope {
        tables: vec![upsert_scope_table(
            target_identifier,
            target_table_id,
            database_id,
            table,
        )],
        right_join_swapped: false,
    };
    let excluded_scope = BindScope {
        tables: vec![upsert_scope_table(
            "excluded".to_string(),
            excluded_table_id,
            database_id,
            table,
        )],
        right_join_swapped: false,
    };
    #[expect(clippy::arc_with_non_send_sync)]
    binder.append_outer_query_scope(Arc::new(excluded_scope), Arc::new(Vec::new()));
    let bind_result = binder.with_phase(BindPhase::NoAliases, |binder| {
        for (column_index, expr) in &mut sets {
            let expected_type = binder
                .resolver
                .schema()
                .get_type_def_unchecked(&table.columns()[*column_index].ty_str)
                .cloned();
            binder.bind_expr_with_expected_type(expr, &target_scope, expected_type.as_deref())?;
        }
        if let Some(where_clause) = where_clause.as_mut() {
            binder.bind_expr(where_clause, &target_scope)?;
        }
        Ok(())
    });
    binder.pop_outer_query_scope();
    bind_result?;

    Ok(BoundUpsertDo::Update { sets, where_clause })
}

/// Bind an INSERT statement up front: validate the target table, resolve
/// defaults in VALUES rows, bind source and RETURNING expressions, and bind
/// every UPSERT conflict target and action.
#[turso_macros::trace_stack]
pub fn bind_insert_stmt(
    tbl_name: &ast::QualifiedName,
    columns: &[ast::Name],
    body: &mut ast::InsertBody,
    returning: &mut Vec<ast::ResultColumn>,
    with_for_returning: &mut Option<ast::With>,
    on_conflict: ast::ResolveType,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<BoundInsert> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(tbl_name)?;
    let table_name = &tbl_name.name;
    let table = match resolver.with_schema(database_id, |s| s.get_table(table_name.as_str())) {
        Some(table) => table,
        None => crate::bail_parse_error!("no such table: {}", table_name),
    };
    if program.trigger.is_some() && table.virtual_table().is_some() {
        crate::bail_parse_error!("unsafe use of virtual table \"{}\"", tbl_name.name.as_str());
    }
    validate_insert(table_name.as_str(), resolver, connection)?;
    if table.virtual_table().is_some() {
        return Ok(BoundInsert {
            values: vec![],
            upsert_actions: vec![],
            inserting_multiple_rows: false,
            database_id,
            table,
            source_select: None,
            returning_cte_definitions: Vec::new(),
            returning_subquery_bindings: HashMap::default(),
            target_table_id: program.table_reference_counter.next(),
            excluded_table_id: program.table_reference_counter.next(),
            bound_index_expressions: Vec::new(),
        });
    }

    let target_table_id = program.table_reference_counter.next();
    let excluded_table_id = program.table_reference_counter.next();
    let value_types = insert_value_types(&table, columns, resolver)?;

    let mut values: Vec<Box<ast::Expr>> = vec![];
    let mut upsert: Option<Box<ast::Upsert>> = None;
    let mut upsert_actions: Vec<BoundUpsertAction> = Vec::new();
    let mut inserting_multiple_rows = false;
    match body {
        ast::InsertBody::DefaultValues => {
            // Generate default values for the table.
            // Check column-level default first, then type-level default.
            let is_strict = table.is_strict();
            values = table
                .columns()
                .iter()
                .filter(|c| !c.hidden() && !c.is_generated())
                .map(|c| {
                    c.default.clone().unwrap_or_else(|| {
                        if let Ok(Some(resolved)) =
                            resolver.schema().resolve_type(&c.ty_str, is_strict)
                        {
                            if let Some(default_expr) = resolved.default_expr() {
                                return Box::new(default_expr.clone());
                            }
                        }
                        Box::new(ast::Expr::Literal(ast::Literal::Null))
                    })
                })
                .collect();
        }
        ast::InsertBody::Select(select, upsert_opt) => {
            // Resolve Expr::Default in all VALUES rows before any compilation.
            if let ast::OneSelect::Values(values_expr) = &mut select.body.select {
                for row in values_expr.iter_mut() {
                    resolve_defaults_in_row(row, &table, columns, resolver);
                }
            }
            for compound in select.body.compounds.iter_mut() {
                if let ast::OneSelect::Values(values_expr) = &mut compound.select {
                    for row in values_expr.iter_mut() {
                        resolve_defaults_in_row(row, &table, columns, resolver);
                    }
                }
            }
            if select.body.compounds.is_empty() {
                match &mut select.body.select {
                    // TODO see how to avoid clone
                    ast::OneSelect::Values(values_expr) if values_expr.len() <= 1 => {
                        if values_expr.is_empty() {
                            crate::bail_parse_error!("no values to insert");
                        }
                        // Check if any VALUES expression contains a subquery.
                        // If so, route through multi-row path which handles subqueries.
                        let has_subquery = values_expr
                            .iter()
                            .any(|row| row.iter().any(|expr| expr_contains_subquery(expr)));
                        if has_subquery {
                            inserting_multiple_rows = true;
                        } else {
                            for expr in values_expr.iter_mut().flat_map(|v| v.iter_mut()) {
                                match expr.as_mut() {
                                    ast::Expr::Id(name) => {
                                        if name.quoted_with('"') && resolver.dqs_dml.is_enabled() {
                                            *expr = ast::Expr::Literal(ast::Literal::String(
                                                name.as_literal(),
                                            ))
                                            .into();
                                        } else {
                                            crate::bail_parse_error!("no such column: {name}");
                                        }
                                    }
                                    ast::Expr::Qualified(first_name, second_name) => {
                                        // an INSERT INTO ... VALUES (...) cannot reference columns
                                        crate::bail_parse_error!(
                                            "no such column: {first_name}.{second_name}"
                                        );
                                    }
                                    _ => {}
                                }
                            }
                            values = values_expr.pop().unwrap_or_else(Vec::new);
                        }
                    }
                    _ => inserting_multiple_rows = true,
                }
            } else {
                inserting_multiple_rows = true;
            }
            upsert = upsert_opt.take();
        }
    }
    if !values.is_empty() {
        let empty_scope = BindScope::empty();
        let function_binder = BindContext::new(resolver, program);
        for (index, expr) in values.iter_mut().enumerate() {
            bind_scopeless_expr(expr, resolver)?;
            function_binder.bind_custom_type_function_calls(
                expr,
                &empty_scope,
                value_types.get(index).and_then(Option::as_deref),
            )?;
        }
    }
    let source_select = if inserting_multiple_rows {
        let ast::InsertBody::Select(select, _) = body else {
            unreachable!("only INSERT SELECT can use the multi-row source path")
        };
        let mut binder = BindContext::new(resolver, program);
        Some(binder.bind_select_with_expected_types(select, &value_types)?)
    } else {
        None
    };
    if let ast::ResolveType::Ignore = on_conflict {
        program.set_resolve_type(ast::ResolveType::Ignore);
        upsert.replace(Box::new(ast::Upsert {
            do_clause: ast::UpsertDo::Nothing,
            index: None,
            next: None,
        }));
    } else {
        program.set_resolve_type(on_conflict);
    }
    let target_identifier =
        normalize_ident(tbl_name.alias.as_ref().unwrap_or(&tbl_name.name).as_str());
    while let Some(mut upsert_opt) = upsert.take() {
        let next = upsert_opt.next.take();
        let action_label = program.allocate_label();
        let mut binder = BindContext::new(resolver, program);
        bind_upsert_conflict_target(&mut binder, &mut upsert_opt, database_id, &table)?;
        let resolved_target = binder.resolver.with_schema(database_id, |schema| {
            super::upsert::resolve_upsert_target(schema, &table, &upsert_opt)
        })?;
        let bound_do = bind_upsert_do(
            &mut binder,
            upsert_opt.do_clause,
            target_identifier.clone(),
            target_table_id,
            excluded_table_id,
            database_id,
            &table,
        )?;
        if !binder.subquery_bindings.is_empty() {
            crate::bail_parse_error!("Subquery is not supported in this position");
        }
        upsert_actions.push((resolved_target, action_label, bound_do));
        upsert = next;
    }
    let (returning_cte_definitions, returning_subquery_bindings) = {
        let mut binder = BindContext::new(resolver, program);
        binder.bind_insert_returning(
            &tbl_name.name,
            target_table_id,
            returning,
            with_for_returning,
            database_id,
        )?
    };
    let bound_index_expressions =
        bind_table_index_expressions(resolver, database_id, table.get_name(), target_table_id);
    Ok(BoundInsert {
        values,
        upsert_actions,
        inserting_multiple_rows,
        database_id,
        table,
        source_select,
        returning_cte_definitions,
        returning_subquery_bindings,
        target_table_id,
        excluded_table_id,
        bound_index_expressions,
    })
}

/// Validate the INSERT target table.
fn validate_insert(
    table_name: &str,
    resolver: &Resolver,
    conn: &Arc<crate::Connection>,
) -> Result<()> {
    // Check if this is a system table that should be protected from direct writes
    if !conn.is_nested_stmt()
        && !conn.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(table_name)
    {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }
    // Check if this is a materialized view
    if resolver.schema().is_materialized_view(table_name) {
        crate::bail_parse_error!("cannot modify materialized view {}", table_name);
    }
    // Check if this table has any incompatible dependent views
    resolver.schema().with_incompatible_dependent_views(table_name, |views| {
    if !views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot DELETE from table '{table_name}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            views.iter().fold(String::new(), |_, s| s.to_string() + ", "),
        );
    }
    Ok(())
    })
}

/// Resolve `Expr::Default` in a VALUES row by replacing it with the column's
/// default expression from the schema.
fn resolve_defaults_in_row(
    row: &mut [Box<ast::Expr>],
    table: &Table,
    columns: &[ast::Name],
    resolver: &Resolver,
) {
    let is_strict = table.is_strict();
    for (i, expr) in row.iter_mut().enumerate() {
        if !matches!(expr.as_ref(), ast::Expr::Default) {
            continue;
        }
        let col = if columns.is_empty() {
            // No column list — position maps to non-hidden columns in order
            table.columns().iter().filter(|c| !c.hidden()).nth(i)
        } else {
            // Column list — map by name
            columns.get(i).and_then(|name| {
                let name = crate::util::normalize_ident(name.as_str());
                table.get_column_by_name(&name).map(|(_, col)| col)
            })
        };
        *expr = match col {
            Some(col) => col.default.clone().unwrap_or_else(|| {
                if let Ok(Some(resolved)) = resolver.schema().resolve_type(&col.ty_str, is_strict) {
                    if let Some(default_expr) = resolved.default_expr() {
                        return Box::new(default_expr.clone());
                    }
                }
                Box::new(ast::Expr::Literal(ast::Literal::Null))
            }),
            None => Box::new(ast::Expr::Literal(ast::Literal::Null)),
        };
    }
}

/// Check if an expression contains a subquery (Subquery, InSelect, or Exists).
/// Used to detect when single-row VALUES should be routed through the
/// multi-row path, which has proper subquery handling.
fn expr_contains_subquery(expr: &ast::Expr) -> bool {
    let mut found_subquery = false;
    let _ = walk_expr(expr, &mut |e| {
        if matches!(
            e,
            ast::Expr::Subquery(_) | ast::Expr::InSelect { .. } | ast::Expr::Exists(_)
        ) {
            found_subquery = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    });
    found_subquery
}

/// Validate the UPDATE target and statement shape.
fn validate_update(
    schema: &crate::schema::Schema,
    body: &ast::Update,
    table_name: &str,
    is_internal_schema_change: bool,
    conn: &Arc<crate::Connection>,
) -> Result<()> {
    // Check if this is a system table that should be protected from direct writes
    if !is_internal_schema_change
        && !conn.is_nested_stmt()
        && !conn.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(table_name)
    {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }
    if !body.order_by.is_empty() {
        crate::bail_parse_error!("ORDER BY is not supported in UPDATE");
    }
    // Check if this is a materialized view
    if schema.is_materialized_view(table_name) {
        crate::bail_parse_error!("cannot modify materialized view {}", table_name);
    }

    // Check if this table has any incompatible dependent views
    schema.with_incompatible_dependent_views(table_name, |views| {
    if !views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot UPDATE table '{table_name}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            views.iter().map(|view| view.as_str()).collect::<Vec<_>>().join(", "),
        );
    }
    Ok(())
    })
}

#[derive(Clone)]
