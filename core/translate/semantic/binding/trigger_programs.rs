struct TriggerColumnBindings {
    table: Arc<BTreeTable>,
    new_registers: Option<Vec<usize>>,
    old_registers: Option<Vec<usize>>,
}

#[derive(Debug)]
struct TriggerParameterAllocator {
    new_columns: Vec<Option<NonZero<usize>>>,
    new_rowid: Option<NonZero<usize>>,
    old_columns: Vec<Option<NonZero<usize>>>,
    old_rowid: Option<NonZero<usize>>,
    next_parameter: usize,
}

impl TriggerParameterAllocator {
    fn new(column_count: usize, has_new: bool, has_old: bool) -> Self {
        Self {
            new_columns: if has_new {
                vec![None; column_count]
            } else {
                Vec::new()
            },
            new_rowid: None,
            old_columns: if has_old {
                vec![None; column_count]
            } else {
                Vec::new()
            },
            old_rowid: None,
            next_parameter: 1,
        }
    }

    fn allocate(slot: &mut Option<NonZero<usize>>, next_parameter: &mut usize) -> NonZero<usize> {
        *slot.get_or_insert_with(|| {
            let parameter = NonZero::new(*next_parameter).expect("parameter indices start at one");
            *next_parameter += 1;
            parameter
        })
    }

    fn new_column(&mut self, index: usize) -> NonZero<usize> {
        Self::allocate(&mut self.new_columns[index], &mut self.next_parameter)
    }

    fn new_rowid(&mut self) -> NonZero<usize> {
        Self::allocate(&mut self.new_rowid, &mut self.next_parameter)
    }

    fn old_column(&mut self, index: usize) -> NonZero<usize> {
        Self::allocate(&mut self.old_columns[index], &mut self.next_parameter)
    }

    fn old_rowid(&mut self) -> NonZero<usize> {
        Self::allocate(&mut self.old_rowid, &mut self.next_parameter)
    }

    fn count(&self) -> usize {
        self.next_parameter - 1
    }
}

/// Binds trigger-body NEW/OLD references to subprogram parameters.
pub struct TriggerProgramBinder {
    parameters: RefCell<TriggerParameterAllocator>,
    has_new: bool,
    has_old: bool,
    table: Arc<BTreeTable>,
    override_conflict: Option<ast::ResolveType>,
    database_name: Option<ast::Name>,
}

impl TriggerProgramBinder {
    pub fn new(
        table: Arc<BTreeTable>,
        has_new: bool,
        has_old: bool,
        override_conflict: Option<ast::ResolveType>,
        database_name: Option<ast::Name>,
    ) -> Self {
        Self {
            parameters: RefCell::new(TriggerParameterAllocator::new(
                table.columns().len(),
                has_new,
                has_old,
            )),
            has_new,
            has_old,
            table,
            override_conflict,
            database_name,
        }
    }

    pub fn bind_command(&self, command: &ast::TriggerCmd) -> Result<ast::Stmt> {
        bind_trigger_command(command, self)
    }

    /// Map each allocated subprogram parameter back to its parent row register.
    pub fn parameter_registers(
        &self,
        new_registers: Option<&[usize]>,
        old_registers: Option<&[usize]>,
    ) -> Vec<usize> {
        let parameters = self.parameters.borrow();
        let mut registers = vec![0; parameters.count()];

        if let Some(new_registers) = new_registers {
            for (column, parameter) in parameters.new_columns.iter().enumerate() {
                if let Some(parameter) = parameter {
                    registers[parameter.get() - 1] = new_registers[column];
                }
            }
            if let Some(parameter) = parameters.new_rowid {
                registers[parameter.get() - 1] = *new_registers
                    .last()
                    .expect("NEW registers include the rowid");
            }
        }

        if let Some(old_registers) = old_registers {
            for (column, parameter) in parameters.old_columns.iter().enumerate() {
                if let Some(parameter) = parameter {
                    registers[parameter.get() - 1] = old_registers[column];
                }
            }
            if let Some(parameter) = parameters.old_rowid {
                registers[parameter.get() - 1] = *old_registers
                    .last()
                    .expect("OLD registers include the rowid");
            }
        }

        registers
    }

    fn new_column_parameter(&self, index: usize) -> Option<NonZero<usize>> {
        self.has_new
            .then(|| self.parameters.borrow_mut().new_column(index))
    }

    fn new_rowid_parameter(&self) -> Option<NonZero<usize>> {
        self.has_new
            .then(|| self.parameters.borrow_mut().new_rowid())
    }

    fn old_column_parameter(&self, index: usize) -> Option<NonZero<usize>> {
        self.has_old
            .then(|| self.parameters.borrow_mut().old_column(index))
    }

    fn old_rowid_parameter(&self) -> Option<NonZero<usize>> {
        self.has_old
            .then(|| self.parameters.borrow_mut().old_rowid())
    }
}

fn trigger_parameter_expr(index: NonZero<usize>, column_type: Option<&str>) -> ast::Expr {
    let index = u32::try_from(index.get())
        .ok()
        .and_then(std::num::NonZeroU32::new)
        .expect("trigger parameter index must fit into NonZeroU32");
    match column_type {
        Some(column_type) => ast::Expr::Variable(ast::Variable::indexed_typed(index, column_type)),
        None => ast::Expr::Variable(ast::Variable::indexed(index)),
    }
}

fn bind_trigger_expression(expr: &mut ast::Expr, binder: &TriggerProgramBinder) -> Result<()> {
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        bind_trigger_expression_node(expr, binder)?;
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn bind_trigger_expression_node(expr: &mut ast::Expr, binder: &TriggerProgramBinder) -> Result<()> {
    match expr {
        ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
            bind_trigger_select_expressions(select, binder)?;
        }
        ast::Expr::InSelect { rhs, .. } => {
            bind_trigger_select_expressions(rhs, binder)?;
        }
        ast::Expr::Qualified(namespace, column)
        | ast::Expr::DoublyQualified(_, namespace, column) => {
            let namespace = normalize_ident(namespace.as_str());
            let column = normalize_ident(column.as_str());
            let column_definition = binder.table.get_column(&column);
            let is_rowid = super::planner::ROWID_STRS
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&column));

            let (parameter, column_type) = if namespace.eq_ignore_ascii_case("new") {
                if !binder.has_new {
                    crate::bail_parse_error!(
                        "NEW references are only valid in INSERT and UPDATE triggers"
                    );
                }
                if let Some((index, definition)) = column_definition {
                    let parameter = if definition.is_rowid_alias() {
                        binder.new_rowid_parameter()
                    } else {
                        binder.new_column_parameter(index)
                    };
                    (parameter, Some(definition.ty_str.as_str()))
                } else if is_rowid {
                    (binder.new_rowid_parameter(), None)
                } else {
                    crate::bail_parse_error!("no such column: {}.{}", namespace, column);
                }
            } else if namespace.eq_ignore_ascii_case("old") {
                if !binder.has_old {
                    crate::bail_parse_error!(
                        "OLD references are only valid in UPDATE and DELETE triggers"
                    );
                }
                if let Some((index, definition)) = column_definition {
                    let parameter = if definition.is_rowid_alias() {
                        binder.old_rowid_parameter()
                    } else {
                        binder.old_column_parameter(index)
                    };
                    (parameter, Some(definition.ty_str.as_str()))
                } else if is_rowid {
                    (binder.old_rowid_parameter(), None)
                } else {
                    crate::bail_parse_error!("no such column: {}.{}", namespace, column);
                }
            } else {
                return Ok(());
            };

            *expr = trigger_parameter_expr(
                parameter.expect("trigger row parameters must be available"),
                column_type,
            );
        }
        _ => {}
    }
    Ok(())
}

fn bind_trigger_upsert(
    upsert: &mut Option<Box<ast::Upsert>>,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    let mut current = upsert.as_mut();
    while let Some(upsert) = current {
        if let ast::UpsertDo::Set { sets, where_clause } = &mut upsert.do_clause {
            for set in sets {
                bind_trigger_expression(&mut set.expr, binder)?;
            }
            if let Some(where_clause) = where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
        }
        if let Some(index) = &mut upsert.index {
            if let Some(where_clause) = &mut index.where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
        }
        current = upsert.next.as_mut();
    }
    Ok(())
}

fn bind_trigger_command(
    command: &ast::TriggerCmd,
    binder: &TriggerProgramBinder,
) -> Result<ast::Stmt> {
    match command {
        ast::TriggerCmd::Insert {
            or_conflict,
            tbl_name,
            col_names,
            select,
            upsert,
            returning,
        } => {
            let mut select = select.clone();
            bind_trigger_select_expressions(&mut select, binder)?;
            let mut upsert = upsert.clone();
            bind_trigger_upsert(&mut upsert, binder)?;
            Ok(ast::Stmt::Insert {
                with: None,
                or_conflict: binder.override_conflict.or(*or_conflict),
                tbl_name: ast::QualifiedName {
                    db_name: binder.database_name.clone(),
                    name: tbl_name.clone(),
                    alias: None,
                },
                columns: col_names.clone(),
                body: ast::InsertBody::Select(select, upsert),
                returning: returning.clone(),
            })
        }
        ast::TriggerCmd::Update {
            or_conflict,
            tbl_name,
            sets,
            from,
            where_clause,
        } => {
            let mut sets = sets.clone();
            for set in &mut sets {
                bind_trigger_expression(&mut set.expr, binder)?;
            }
            let mut from = from.clone();
            if let Some(from) = &mut from {
                bind_trigger_from_expressions(from, binder)?;
            }
            let mut where_clause = where_clause.clone();
            if let Some(where_clause) = &mut where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
            Ok(ast::Stmt::Update(ast::Update {
                with: None,
                or_conflict: binder.override_conflict.or(*or_conflict),
                tbl_name: ast::QualifiedName {
                    db_name: binder.database_name.clone(),
                    name: tbl_name.clone(),
                    alias: None,
                },
                indexed: None,
                sets,
                from,
                where_clause,
                returning: Vec::new(),
                order_by: Vec::new(),
                limit: None,
            }))
        }
        ast::TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } => {
            let mut where_clause = where_clause.clone();
            if let Some(where_clause) = &mut where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
            Ok(ast::Stmt::Delete {
                tbl_name: ast::QualifiedName {
                    db_name: binder.database_name.clone(),
                    name: tbl_name.clone(),
                    alias: None,
                },
                where_clause,
                limit: None,
                returning: Vec::new(),
                indexed: None,
                order_by: Vec::new(),
                with: None,
            })
        }
        ast::TriggerCmd::Select(select) => {
            let mut select = select.clone();
            bind_trigger_select_expressions(&mut select, binder)?;
            Ok(ast::Stmt::Select(select))
        }
    }
}

fn bind_trigger_select_expressions(
    select: &mut ast::Select,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            bind_trigger_select_expressions(&mut cte.select, binder)?;
        }
    }
    bind_trigger_one_select_expressions(&mut select.body.select, binder)?;
    for compound in &mut select.body.compounds {
        bind_trigger_one_select_expressions(&mut compound.select, binder)?;
    }
    for order_by in &mut select.order_by {
        bind_trigger_expression(&mut order_by.expr, binder)?;
    }
    if let Some(limit) = &mut select.limit {
        bind_trigger_expression(&mut limit.expr, binder)?;
        if let Some(offset) = &mut limit.offset {
            bind_trigger_expression(offset, binder)?;
        }
    }
    Ok(())
}

fn bind_trigger_one_select_expressions(
    select: &mut ast::OneSelect,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    match select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            for column in columns {
                if let ast::ResultColumn::Expr(expr, _) = column {
                    bind_trigger_expression(expr, binder)?;
                }
            }
            if let Some(from) = from {
                bind_trigger_from_expressions(from, binder)?;
            }
            if let Some(where_clause) = where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
            if let Some(group_by) = group_by {
                for expr in &mut group_by.exprs {
                    bind_trigger_expression(expr, binder)?;
                }
                if let Some(having) = &mut group_by.having {
                    bind_trigger_expression(having, binder)?;
                }
            }
            for window in window_clause {
                bind_trigger_window_expressions(&mut window.window, binder)?;
            }
        }
        ast::OneSelect::Values(rows) => {
            for row in rows {
                for expr in row {
                    bind_trigger_expression(expr, binder)?;
                }
            }
        }
    }
    Ok(())
}

fn bind_trigger_from_expressions(
    from: &mut ast::FromClause,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    bind_trigger_select_table_expressions(&mut from.select, binder)?;
    for join in &mut from.joins {
        bind_trigger_select_table_expressions(&mut join.table, binder)?;
        if let Some(ast::JoinConstraint::On(expr)) = &mut join.constraint {
            bind_trigger_expression(expr, binder)?;
        }
    }
    Ok(())
}

fn bind_trigger_select_table_expressions(
    table: &mut ast::SelectTable,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(..) => {}
        ast::SelectTable::TableCall(_, arguments, _) => {
            for argument in arguments {
                bind_trigger_expression(argument, binder)?;
            }
        }
        ast::SelectTable::Select(select, _) => {
            bind_trigger_select_expressions(select, binder)?;
        }
        ast::SelectTable::Sub(from, _) => {
            bind_trigger_from_expressions(from, binder)?;
        }
    }
    Ok(())
}

fn bind_trigger_window_expressions(
    window: &mut ast::Window,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    for expr in &mut window.partition_by {
        bind_trigger_expression(expr, binder)?;
    }
    for order_by in &mut window.order_by {
        bind_trigger_expression(&mut order_by.expr, binder)?;
    }
    if let Some(frame) = &mut window.frame_clause {
        bind_trigger_frame_bound(&mut frame.start, binder)?;
        if let Some(end) = &mut frame.end {
            bind_trigger_frame_bound(end, binder)?;
        }
    }
    Ok(())
}

fn bind_trigger_frame_bound(
    bound: &mut ast::FrameBound,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            bind_trigger_expression(expr, binder)
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => Ok(()),
    }
}

