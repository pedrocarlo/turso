// ── Statement-level binding ──────────────────────────────────────────────

/// Bind a SELECT statement before planning or emission.
pub fn bind_select_stmt(
    select: &mut ast::Select,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<BoundSelect> {
    let mut binder = BindContext::new(resolver, program);
    binder.bind_select(select)
}

/// Bind NEW/OLD references and subqueries in a trigger WHEN clause.
pub fn bind_trigger_when_clause(
    expr: &mut ast::Expr,
    table: Arc<BTreeTable>,
    new_registers: Option<&[usize]>,
    old_registers: Option<&[usize]>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<HashMap<ast::TableInternalId, BoundSubquery>> {
    let mut binder = BindContext::new(resolver, program);
    binder.bind_trigger_when(expr, table, new_registers, old_registers)
}

/// Bind a DELETE statement up front: validate the target table and resolve
/// all names in WHERE and RETURNING. Planning consumes the result without
/// re-resolving anything.
#[allow(clippy::too_many_arguments)]
pub fn bind_delete_stmt(
    tbl_name: &ast::QualifiedName,
    indexed: Option<ast::Indexed>,
    where_clause: &mut Option<Box<ast::Expr>>,
    returning: &mut Vec<ast::ResultColumn>,
    with: &mut Option<ast::With>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<BoundDelete> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(tbl_name)?;
    let normalized_table_name = normalize_ident(tbl_name.name.as_str());
    let table = validate_delete(
        resolver,
        &normalized_table_name,
        database_id,
        program,
        connection,
    )?;
    let mut binder = BindContext::new(resolver, program);
    binder.bind_delete(
        tbl_name,
        indexed,
        where_clause,
        returning,
        with,
        database_id,
        table,
    )
}

/// Bind an UPDATE statement up front: validate the target table and resolve
/// all names in FROM/SET/WHERE/RETURNING. Planning consumes the result
/// without re-resolving anything.
pub fn bind_update_stmt(
    body: &mut ast::Update,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    is_internal_schema_change: bool,
) -> Result<BoundUpdate> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(&body.tbl_name)?;
    let target_name = &body.tbl_name.name;
    let table = match resolver.with_schema(database_id, |s| s.get_table(target_name.as_str())) {
        Some(table) => table,
        None => crate::bail_parse_error!("Parse error: no such table: {}", target_name),
    };
    if program.trigger.is_some() && table.virtual_table().is_some() {
        crate::bail_parse_error!(
            "unsafe use of virtual table \"{}\"",
            body.tbl_name.name.as_str()
        );
    }
    if table.btree().is_some_and(|bt| !bt.has_rowid) {
        crate::bail_parse_error!("UPDATE of WITHOUT ROWID tables is not supported");
    }
    validate_update(
        resolver.schema(),
        body,
        target_name.as_str(),
        is_internal_schema_change,
        connection,
    )?;
    let mut binder = BindContext::new(resolver, program);
    binder.bind_update(body, database_id, table)
}

/// Validate the DELETE target, returning the underlying table if validation
/// passes.
fn validate_delete(
    resolver: &Resolver,
    tbl_name: &str,
    database_id: usize,
    program: &ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<Arc<Table>> {
    // Check if this is a system table that should be protected from direct writes
    if !connection.is_nested_stmt()
        && !connection.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(tbl_name)
    {
        crate::bail_parse_error!("table {tbl_name} may not be modified");
    }
    let table = match resolver.with_schema(database_id, |s| s.get_table(tbl_name)) {
        Some(table) => table,
        None => crate::bail_parse_error!("no such table: {}", tbl_name),
    };
    if program.trigger.is_some() && table.virtual_table().is_some() {
        crate::bail_parse_error!("unsafe use of virtual table \"{}\"", tbl_name);
    }
    if table.btree().is_some_and(|bt| !bt.has_rowid) {
        crate::bail_parse_error!("DELETE from WITHOUT ROWID tables is not supported");
    }

    // Check if this is a materialized view
    if resolver.schema().is_materialized_view(tbl_name) {
        crate::bail_parse_error!("cannot modify materialized view {}", tbl_name);
    }

    // Check if this table has any incompatible dependent views
    resolver.schema().with_incompatible_dependent_views(tbl_name, |views| {
    if !views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot DELETE from table '{tbl_name}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            views.iter().fold(String::new(), |_, s| s.to_string() + ", "),
        );
    }
    Ok(())
    })?;
    Ok(table)
}

