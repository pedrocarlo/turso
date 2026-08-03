pub struct LogicalScopeColumn<'a> {
    pub name: &'a str,
    pub database: Option<&'a str>,
    pub table: Option<&'a str>,
    pub table_alias: Option<&'a str>,
}

pub struct BoundLogicalColumn {
    pub name: String,
    pub table: Option<String>,
}

pub enum BoundLogicalSource {
    CommonTableExpression {
        name: String,
    },
    Table {
        database: Option<String>,
        name: String,
        table: Arc<Table>,
    },
}

/// Bind a logical-planner table source against visible CTEs and the schema.
pub fn bind_logical_source<'a>(
    source: &ast::QualifiedName,
    cte_names: impl IntoIterator<Item = &'a str>,
    schema: &Schema,
) -> Result<BoundLogicalSource> {
    let database = source.db_name.as_ref().map(|name| name.as_str());
    let name = source.name.as_str();

    if database.is_none() {
        if let Some(cte_name) = cte_names
            .into_iter()
            .find(|cte_name| cte_name.eq_ignore_ascii_case(name))
        {
            return Ok(BoundLogicalSource::CommonTableExpression {
                name: cte_name.to_string(),
            });
        }
    }

    let Some(table) = schema.get_table(name) else {
        let qualified_name =
            database.map_or_else(|| name.to_string(), |database| format!("{database}.{name}"));
        crate::bail_parse_error!("no such table: {}", qualified_name);
    };
    let name = table.get_name().to_string();
    Ok(BoundLogicalSource::Table {
        database: database.map(str::to_string),
        name,
        table,
    })
}

/// Bind a raw logical-planner column expression against its input schema.
pub fn bind_logical_column<'a>(
    expr: &ast::Expr,
    columns: impl IntoIterator<Item = LogicalScopeColumn<'a>>,
) -> Result<Option<BoundLogicalColumn>> {
    let (database, table, column) = match expr {
        ast::Expr::Id(column) | ast::Expr::Name(column) => (None, None, column.as_str()),
        ast::Expr::Qualified(table, column) => (None, Some(table.as_str()), column.as_str()),
        ast::Expr::DoublyQualified(database, table, column) => (
            Some(database.as_str()),
            Some(table.as_str()),
            column.as_str(),
        ),
        _ => return Ok(None),
    };

    let mut matches = columns
        .into_iter()
        .filter(|candidate| candidate.name.eq_ignore_ascii_case(column))
        .filter(|candidate| {
            let table_matches = table.is_none_or(|table| {
                candidate
                    .table_alias
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(table))
                    || candidate
                        .table
                        .is_some_and(|name| name.eq_ignore_ascii_case(table))
            });
            let database_matches = database.is_none_or(|database| {
                candidate
                    .database
                    .is_some_and(|name| name.eq_ignore_ascii_case(database))
            });
            table_matches && database_matches
        });
    let Some(bound) = matches.next() else {
        let name = match (database, table) {
            (Some(database), Some(table)) => format!("{database}.{table}.{column}"),
            (None, Some(table)) => format!("{table}.{column}"),
            _ => column.to_string(),
        };
        crate::bail_parse_error!("no such column: {}", name);
    };
    if matches.next().is_some() {
        crate::bail_parse_error!("ambiguous column name: {}", column);
    }

    let table = if let (Some(database), Some(table)) = (bound.database, bound.table) {
        Some(format!("{database}.{table}"))
    } else if let Some(alias) = bound.table_alias {
        Some(alias.to_string())
    } else {
        bound.table.map(str::to_string)
    };
    Ok(Some(BoundLogicalColumn {
        name: bound.name.to_string(),
        table,
    }))
}

fn bind_table_index_expressions(
    resolver: &Resolver,
    database_id: usize,
    table_name: &str,
    internal_id: ast::TableInternalId,
) -> Vec<super::plan::BoundIndexExpressions> {
    resolver
        .with_schema(database_id, |schema| {
            schema.get_indices(table_name).cloned().collect::<Vec<_>>()
        })
        .into_iter()
        .map(|index| {
            let mut columns = index
                .columns
                .iter()
                .map(|column| column.expr.clone())
                .collect::<Vec<_>>();
            let mut where_clause = index.where_clause.clone();
            for expr in columns.iter_mut().flatten() {
                rebase_schema_expr(expr, internal_id);
                rewrite_between_expressions(expr);
            }
            if let Some(expr) = where_clause.as_mut() {
                rebase_schema_expr(expr, internal_id);
                rewrite_between_expressions(expr);
            }
            super::plan::BoundIndexExpressions {
                index_name: index.name.clone(),
                columns,
                where_clause,
            }
        })
        .collect()
}

/// Validate a referenced CTE's explicit column list against its SELECT's
/// result column count. SQLite defers this check until the CTE is actually
/// referenced, so unreferenced CTEs with mismatched counts don't error.
fn validate_cte_explicit_columns(name: &str, cte: &CteEntry) -> Result<()> {
    if !cte.explicit_columns.is_empty()
        && cte.result_column_count != 0
        && cte.explicit_columns.len() != cte.result_column_count
    {
        crate::bail_parse_error!(
            "table {} has {} values for {} columns",
            name,
            cte.result_column_count,
            cte.explicit_columns.len()
        );
    }
    Ok(())
}

