fn extract_index_collation<'a>(
    expr: &'a ast::Expr,
    resolver: Option<&Resolver>,
) -> Result<(Option<super::collate::CollationSeq>, &'a ast::Expr)> {
    let mut current = expr;
    let mut collation = None;
    loop {
        current = unwrap_parens(current)?;
        match current {
            ast::Expr::Collate(inner, name) => {
                if collation.is_none() {
                    let resolved = match resolver {
                        Some(resolver) => resolver.resolve_collation(name.as_str())?,
                        None => super::collate::CollationSeq::new(name.as_str())?,
                    };
                    if resolved.is_custom() {
                        crate::bail_parse_error!("custom collations are not supported in indexes");
                    }
                    collation = Some(resolved);
                }
                current = inner.as_ref();
            }
            _ => return Ok((collation, current)),
        }
    }
}

fn bind_index_column<'a>(
    expr: &'a ast::Expr,
    table: &'a BTreeTable,
) -> Option<(usize, String, &'a Column)> {
    let (position, column) = match expr {
        ast::Expr::Id(column_name) | ast::Expr::Name(column_name) => {
            table.get_column(column_name.as_str())?
        }
        // SQLite keeps this backwards-compatibility behavior for a bare string key.
        ast::Expr::Literal(ast::Literal::String(column_name)) => {
            table.get_column(column_name.trim_matches('\''))?
        }
        ast::Expr::Qualified(_, column) | ast::Expr::DoublyQualified(_, _, column) => {
            table.get_column(column.as_str())?
        }
        ast::Expr::RowId { .. } => table.get_rowid_alias_column()?,
        _ => return None,
    };
    let column_name = column
        .name
        .as_ref()
        .expect("indexed column must have a name")
        .clone();
    Some((position, column_name, column))
}

fn is_valid_index_expression(expr: &ast::Expr, table: &BTreeTable) -> bool {
    if matches!(expr, ast::Expr::Literal(ast::Literal::String(_))) {
        return false;
    }

    let table_name = normalize_ident(table.name.as_str());
    let has_column = |name: &str| {
        let name = normalize_ident(name);
        table.columns().iter().any(|column| {
            column
                .name
                .as_ref()
                .is_some_and(|column_name| normalize_ident(column_name) == name)
        })
    };
    let is_table = |name: &str| normalize_ident(name).eq_ignore_ascii_case(&table_name);
    let is_deterministic_function = |name: &str, args: &[Box<ast::Expr>]| {
        let name = normalize_ident(name);
        Func::resolve_function(&name, args.len()).is_ok_and(|function| {
            function.is_some_and(|function| is_deterministic_schema_function_call(&function, args))
        })
    };

    let mut valid = true;
    let _ = walk_expr(expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
        if !valid {
            return Ok(WalkControl::SkipChildren);
        }
        match expr {
            ast::Expr::Literal(
                ast::Literal::CurrentDate
                | ast::Literal::CurrentTime
                | ast::Literal::CurrentTimestamp,
            ) => valid = false,
            ast::Expr::Literal(_) | ast::Expr::RowId { .. } => {}
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                if !has_column(name.as_str()) {
                    valid = false;
                }
            }
            ast::Expr::Qualified(namespace, column)
            | ast::Expr::DoublyQualified(_, namespace, column) => {
                if !is_table(namespace.as_str()) || !has_column(column.as_str()) {
                    valid = false;
                }
            }
            ast::Expr::FunctionCall {
                name, filter_over, ..
            }
            | ast::Expr::FunctionCallStar {
                name, filter_over, ..
            } => {
                if filter_over.over_clause.is_some() {
                    valid = false;
                } else {
                    let args = match expr {
                        ast::Expr::FunctionCall { args, .. } => args.as_slice(),
                        ast::Expr::FunctionCallStar { .. } => &[] as &[Box<ast::Expr>],
                        _ => unreachable!(),
                    };
                    if !is_deterministic_function(name.as_str(), args) {
                        valid = false;
                    }
                }
            }
            ast::Expr::Exists(_)
            | ast::Expr::InSelect { .. }
            | ast::Expr::Subquery(_)
            | ast::Expr::Raise { .. }
            | ast::Expr::Variable(_) => valid = false,
            _ => {}
        }
        Ok(if valid {
            WalkControl::Continue
        } else {
            WalkControl::SkipChildren
        })
    });
    valid
}

fn bind_self_table_leaf(name: &str, table: &BTreeTable) -> Option<ast::Expr> {
    if let Some((column, definition)) = table.get_column(name) {
        return Some(ast::Expr::Column {
            database: None,
            table: ast::TableInternalId::SELF_TABLE,
            column,
            is_rowid_alias: definition.is_rowid_alias(),
        });
    }
    if super::planner::ROWID_STRS
        .iter()
        .any(|rowid| rowid.eq_ignore_ascii_case(name))
    {
        return Some(ast::Expr::RowId {
            database: None,
            table: ast::TableInternalId::SELF_TABLE,
        });
    }
    None
}

fn first_unbound_identifier(expr: &ast::Expr) -> Option<String> {
    let mut found = None;
    let _ = walk_expr(expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
        if found.is_some() {
            return Ok(WalkControl::SkipChildren);
        }
        match expr {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                found = Some(name.as_str().to_string());
                Ok(WalkControl::SkipChildren)
            }
            ast::Expr::Qualified(namespace, column)
            | ast::Expr::DoublyQualified(_, namespace, column) => {
                found = Some(format!("{}.{}", namespace.as_str(), column.as_str()));
                Ok(WalkControl::SkipChildren)
            }
            _ => Ok(WalkControl::Continue),
        }
    });
    found
}

