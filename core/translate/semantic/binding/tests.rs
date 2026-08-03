#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{BTreeTable, Schema};
    use crate::{DatabaseCatalog, RwLock, SymbolTable};
    use turso_parser::ast::{Cmd, Stmt};
    use turso_parser::parser::Parser;

    #[derive(Default)]
    struct TestIdGenerator {
        next: usize,
    }

    impl IdGenerator for TestIdGenerator {
        fn next_table_id(&mut self) -> TableInternalId {
            let id = self.next;
            self.next += 1;
            id.into()
        }

        fn next_cte_id(&mut self) -> usize {
            let id = self.next;
            self.next += 1;
            id
        }
    }

    fn parse_select(sql: &str) -> ast::Select {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser
            .next_cmd()
            .expect("SQL should parse")
            .expect("SQL should contain a statement");
        match cmd {
            Cmd::Stmt(Stmt::Select(select)) => select,
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    fn with_bind_context<T>(
        table_ddls: &[&str],
        f: impl FnOnce(&mut BindContext<'_, TestIdGenerator>) -> T,
    ) -> T {
        let mut schema = Schema::new();
        for (idx, ddl) in table_ddls.iter().enumerate() {
            schema
                .add_btree_table(Arc::new(
                    BTreeTable::from_sql(ddl, (idx + 2) as i64).expect("table DDL should parse"),
                ))
                .expect("table should be added to schema");
        }

        with_schema_bind_context(&schema, false, f)
    }

    fn with_schema_bind_context<T>(
        schema: &Schema,
        enable_custom_types: bool,
        f: impl FnOnce(&mut BindContext<'_, TestIdGenerator>) -> T,
    ) -> T {
        let database_schemas = RwLock::new(HashMap::default());
        let temp_database = RwLock::new(None);
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let symbol_table = SymbolTable::new();
        let resolver = Resolver::new(
            &schema,
            &database_schemas,
            &temp_database,
            &attached_databases,
            &symbol_table,
            enable_custom_types,
            crate::translate::emitter::DoubleQuotedDml::Enabled,
            crate::sync::Arc::new(crate::dialect::SqliteDialect),
        );
        let mut id_gen = TestIdGenerator::default();
        let mut ctx = BindContext::new(&resolver, &mut id_gen);
        f(&mut ctx)
    }

    #[test]
    fn nested_custom_field_access_is_fully_bound() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE telegram_msg AS STRUCT(chat_id INT, text TEXT)")
            .unwrap();
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram telegram_msg, slack TEXT)")
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE msgs(id INT, data platform) STRICT", 2).unwrap(),
            ))
            .unwrap();

        with_schema_bind_context(&schema, true, |ctx| {
            let mut select = parse_select("SELECT data.telegram.chat_id FROM msgs");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::FieldAccess {
                base,
                resolved: Some(ast::FieldAccessResolution::StructField { field_index: 0 }),
                ..
            } = select_expr(&select, 0)
            else {
                panic!("expected bound struct field access");
            };
            let ast::Expr::FieldAccess {
                base,
                resolved: Some(ast::FieldAccessResolution::UnionVariant { tag_index: 0 }),
                ..
            } = base.as_ref()
            else {
                panic!("expected bound union variant access");
            };
            assert_column_expr(base, 0, 1);
        });
    }

    #[test]
    fn custom_type_extraction_functions_are_fully_bound() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE telegram_msg AS STRUCT(chat_id INT, text TEXT)")
            .unwrap();
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram telegram_msg, slack TEXT)")
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE msgs(id INT, data platform) STRICT", 2).unwrap(),
            ))
            .unwrap();

        with_schema_bind_context(&schema, true, |ctx| {
            let mut select = parse_select(
                "SELECT union_tag(data), \
                 struct_extract(union_extract(data, 'telegram'), 'chat_id') FROM msgs",
            );
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::BoundCustomTypeFunction {
                resolution: ast::CustomTypeFunctionResolution::UnionTag { tag_names },
                ..
            } = select_expr(&select, 0)
            else {
                panic!("expected bound union_tag call");
            };
            assert_eq!(tag_names.as_ref(), ["telegram", "slack"]);

            let ast::Expr::BoundCustomTypeFunction {
                call,
                resolution: ast::CustomTypeFunctionResolution::StructExtract { field_index: 0, .. },
            } = select_expr(&select, 1)
            else {
                panic!("expected bound struct_extract call");
            };
            let ast::Expr::FunctionCall { args, .. } = call.as_ref() else {
                panic!("expected wrapped struct_extract call");
            };
            assert!(matches!(
                args[0].as_ref(),
                ast::Expr::BoundCustomTypeFunction {
                    resolution: ast::CustomTypeFunctionResolution::UnionExtract {
                        tag_index: 0,
                        ..
                    },
                    ..
                }
            ));
        });
    }

    #[test]
    fn invalid_union_extract_fails_during_binding() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram TEXT, slack TEXT)")
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE msgs(id INT, data platform) STRICT", 2).unwrap(),
            ))
            .unwrap();

        with_schema_bind_context(&schema, true, |ctx| {
            let error = bind_select_error(ctx, "SELECT union_extract(data, 'discord') FROM msgs")
                .to_string();
            assert!(
                error.contains("unknown variant 'discord' in union type 'platform'"),
                "unexpected error: {error}"
            );
        });
    }

    #[test]
    fn invalid_custom_field_access_fails_during_binding() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE point AS STRUCT(x INT, y INT)")
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE points(id INT, pos point) STRICT", 2).unwrap(),
            ))
            .unwrap();

        with_schema_bind_context(&schema, true, |ctx| {
            let error = bind_select_error(ctx, "SELECT pos.z FROM points").to_string();
            assert!(
                error.contains("no such field 'z' in struct type 'point'"),
                "unexpected error: {error}"
            );
        });
    }

    fn select_expr(select: &ast::Select, idx: usize) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { columns, .. } => match &columns[idx] {
                ast::ResultColumn::Expr(expr, _) => expr,
                other => panic!("expected expression result column, got {other:?}"),
            },
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn where_expr(select: &ast::Select) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { where_clause, .. } => where_clause
                .as_deref()
                .expect("expected WHERE clause on bound select"),
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn group_by_expr(select: &ast::Select, idx: usize) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { group_by, .. } => {
                &group_by.as_ref().expect("expected GROUP BY clause").exprs[idx]
            }
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn having_expr(select: &ast::Select) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { group_by, .. } => group_by
                .as_ref()
                .and_then(|group_by| group_by.having.as_deref())
                .expect("expected HAVING clause"),
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn order_by_expr(select: &ast::Select, idx: usize) -> &ast::Expr {
        &select.order_by[idx].expr
    }

    fn exists_subquery_id(select: &ast::Select) -> TableInternalId {
        match where_expr(select) {
            ast::Expr::SubqueryResult { subquery_id, .. } => *subquery_id,
            other => panic!("expected SubqueryResult in WHERE, got {other:?}"),
        }
    }

    fn subquery_id_from_expr(expr: &ast::Expr) -> TableInternalId {
        match expr {
            ast::Expr::SubqueryResult { subquery_id, .. } => *subquery_id,
            other => panic!("expected SubqueryResult expression, got {other:?}"),
        }
    }

    fn assert_column_expr(expr: &ast::Expr, table: usize, column: usize) {
        assert_eq!(
            expr,
            &ast::Expr::Column {
                database: None,
                table: TableInternalId::from(table),
                column,
                is_rowid_alias: false,
            }
        );
    }

    fn bind_select_error(
        ctx: &mut BindContext<'_, TestIdGenerator>,
        sql: &str,
    ) -> crate::LimboError {
        let mut select = parse_select(sql);
        match ctx.bind_select(&mut select) {
            Ok(_) => panic!("expected bind failure for SQL: {sql}"),
            Err(err) => err,
        }
    }

    #[test]
    fn bind_select_returns_main_scope_and_tracking() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT b FROM t WHERE a = 1 ORDER BY b");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_scope.tables.len(), 1);
            assert_eq!(bound.main_scope.tables[0].identifier, "t");
            assert_eq!(
                bound.main_scope.tables[0].internal_id,
                TableInternalId::from(0usize)
            );
            assert_eq!(bound.tracking.columns_used.len(), 2);
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 0)));
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 1)));
        });
    }

    #[test]
    fn bind_select_keeps_subquery_tracking_out_of_outer_tracking() {
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b)"], |ctx| {
            let mut select =
                parse_select("SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.b = a)");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(
                bound.tracking.columns_used,
                vec![(TableInternalId::from(0usize), 0)]
            );
            assert!(bound.tracking.outer_refs_used.is_empty());
        });
    }

    #[test]
    fn bound_select_into_table_references_populates_joined_tables() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT b FROM t WHERE a = 1");
            let bound = ctx.bind_select(&mut select).unwrap();
            let mut all_refs = bound
                .into_table_references_with_outer_refs(
                    &mut HashMap::default(),
                    &mut HashMap::default(),
                    Vec::new(),
                )
                .unwrap();

            assert_eq!(all_refs.len(), 1);
            let table_references = all_refs.remove(0);
            assert_eq!(table_references.joined_tables().len(), 1);
            let table = &table_references.joined_tables()[0];
            assert_eq!(table.identifier, "t");
            assert_eq!(table.internal_id, TableInternalId::from(0usize));
            assert_eq!(table.table.get_name(), "t");
            assert!(table.col_used_mask.get(0));
            assert!(table.col_used_mask.get(1));
            assert!(table_references.outer_query_refs().is_empty());
        });
    }

    #[test]
    fn bind_cte_uses_bound_select_result_columns() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select =
                parse_select("WITH cte(col_x, col_y) AS (SELECT x, y FROM t) SELECT * FROM cte");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let cte = ctx.get_cte("cte").expect("cte should exist");
            assert_eq!(cte.resolved_columns, vec!["col_x", "col_y"]);
        });
    }

    #[test]
    fn bind_cte_allocates_cte_id_and_stores_inner_bound() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select =
                parse_select("WITH c AS (SELECT a, b FROM t WHERE a > 1) SELECT * FROM c");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let cte = ctx.get_cte("c").expect("cte should exist");
            // cte_id was allocated
            assert_eq!(cte.cte_id, 0);
            // inner_bound is populated
            assert!(cte.inner_bound.is_some());
            let inner = cte.inner_bound.as_ref().unwrap();
            assert_eq!(inner.main_scope.tables.len(), 1);
            assert_eq!(inner.main_scope.tables[0].identifier, "t");
            // resolved columns inferred from SELECT list
            assert_eq!(cte.resolved_columns, vec!["a", "b"]);
        });
    }

    #[test]
    fn bind_cte_tracks_referenced_cte_indices() {
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut select =
                parse_select("WITH a AS (SELECT x FROM t), b AS (SELECT * FROM a) SELECT * FROM b");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let a = ctx.get_cte("a").expect("cte a should exist");
            assert!(a.referenced_cte_indices.is_empty());

            let b = ctx.get_cte("b").expect("cte b should exist");
            assert_eq!(b.referenced_cte_indices.as_slice(), &[0]);
        });
    }

    #[test]
    fn bind_cte_materialize_hint() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select =
                parse_select("WITH c AS MATERIALIZED (SELECT a FROM t) SELECT * FROM c");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let cte = ctx.get_cte("c").expect("cte should exist");
            assert!(cte.materialize_hint);
        });
    }

    #[test]
    fn select_list_uses_no_aliases_phase() {
        with_bind_context(&["CREATE TABLE t(x, a)"], |ctx| {
            let mut select = parse_select("SELECT a AS x, x FROM t");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_column_expr(select_expr(&select, 0), 0, 1);
            assert_column_expr(select_expr(&select, 1), 0, 0);
            assert_eq!(bound.result_columns[0].name, "x");
            assert_eq!(bound.result_columns[1].name, "x");
        });
    }

    #[test]
    fn where_clause_prefers_table_column_over_alias() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a AS b FROM t WHERE b = 1");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = where_expr(&select) else {
                panic!("expected bound WHERE binary expression");
            };
            assert_column_expr(lhs, 0, 1);
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("1".into()))
            );
        });
    }

    #[test]
    fn where_clause_falls_back_to_alias_when_no_table_column_matches() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS x FROM t WHERE x = 3");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = where_expr(&select) else {
                panic!("expected bound WHERE binary expression");
            };
            assert_eq!(
                lhs.as_ref(),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("3".into()))
            );
        });
    }

    #[test]
    fn group_by_prefers_source_column_over_alias() {
        // In GROUP BY, real columns take precedence over SELECT aliases
        // (matches SQLite): `b` resolves to column t.b, not alias (a + 1).
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS b FROM t GROUP BY b");
            ctx.bind_select(&mut select).unwrap();

            assert_column_expr(group_by_expr(&select, 0), 0, 1);
        });
    }

    #[test]
    fn having_prefers_alias_expression_over_table_column() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS b FROM t GROUP BY c HAVING b > 10");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(&select) else {
                panic!("expected bound HAVING binary expression");
            };
            assert_eq!(
                lhs.as_ref(),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("10".into()))
            );
        });
    }

    #[test]
    fn order_by_prefers_alias_expression_over_table_column() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS b FROM t ORDER BY b");
            ctx.bind_select(&mut select).unwrap();

            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
        });
    }

    #[test]
    fn order_by_falls_back_to_main_scope_column_when_alias_is_missing() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a AS renamed FROM t ORDER BY b");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_column_expr(order_by_expr(&select, 0), 0, 1);
            assert_eq!(bound.tracking.columns_used.len(), 2);
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 0)));
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 1)));
        });
    }

    #[test]
    fn correlated_grouped_subquery_binds_inner_aliases_and_outer_references() {
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select(
                "SELECT t.a \
                 FROM t \
                 WHERE EXISTS (\
                    SELECT u.c + 2 AS a \
                    FROM u \
                    WHERE u.b = t.a \
                    GROUP BY a \
                    HAVING a > t.a \
                    ORDER BY a\
                 )",
            );
            let bound = ctx.bind_select(&mut select).unwrap();
            let sq_id = exists_subquery_id(&select);
            let subquery = &bound.subquery_bindings[&sq_id].select;

            assert_eq!(
                bound.tracking.columns_used,
                vec![(TableInternalId::from(0usize), 0)]
            );
            assert!(bound.tracking.outer_refs_used.is_empty());

            // t=0, subquery_id=1, u=2
            assert_eq!(
                select_expr(subquery, 0),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(2usize),
                        column: 1,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("2".into())).into_boxed(),
                )
            );

            let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = where_expr(subquery) else {
                panic!("expected bound inner WHERE binary expression");
            };
            assert_column_expr(lhs, 2, 0);
            assert_column_expr(rhs, 0, 0);

            assert_eq!(group_by_expr(subquery, 0), select_expr(subquery, 0));

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(subquery) else {
                panic!("expected bound inner HAVING binary expression");
            };
            assert_eq!(lhs.as_ref(), select_expr(subquery, 0));
            assert_column_expr(rhs, 0, 0);

            assert_eq!(order_by_expr(subquery, 0), select_expr(subquery, 0));
        });
    }

    #[test]
    fn derived_table_columns_flow_into_outer_alias_binding() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select(
                "SELECT sq.x AS y \
                 FROM (SELECT t.a + 1 AS x FROM t) AS sq \
                 WHERE y > 2 \
                 ORDER BY y",
            );
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_scope.tables.len(), 1);
            assert_eq!(bound.main_scope.tables[0].identifier, "sq");

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = where_expr(&select) else {
                panic!("expected bound outer WHERE binary expression");
            };
            assert_eq!(
                lhs.as_ref(),
                &ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(1usize),
                    column: 0,
                    is_rowid_alias: false,
                }
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("2".into()))
            );

            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(1usize),
                    column: 0,
                    is_rowid_alias: false,
                }
            );
        });
    }

    #[test]
    fn cte_query_combines_cte_scope_group_by_having_and_order_by() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select(
                "WITH cte AS (SELECT a, b FROM t) \
                 SELECT a + 1 AS b \
                 FROM cte \
                 GROUP BY b \
                 HAVING b > 2 \
                 ORDER BY b",
            );
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_scope.tables.len(), 1);
            assert_eq!(bound.main_scope.tables[0].identifier, "cte");
            assert!(matches!(
                bound.main_scope.tables[0].source,
                ScopeTableSource::Cte { .. }
            ));

            // cte_id=0, t (inside CTE body)=1, cte (outer FROM)=2
            let alias_expr = ast::Expr::Binary(
                ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(2usize),
                    column: 0,
                    is_rowid_alias: false,
                }
                .into_boxed(),
                ast::Operator::Add,
                ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
            );

            assert_eq!(select_expr(&select, 0), &alias_expr);
            // GROUP BY prefers real columns over aliases (matches SQLite):
            // `b` resolves to column cte.b, not the alias expression (a + 1).
            assert_eq!(
                group_by_expr(&select, 0),
                &ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(2usize),
                    column: 1,
                    is_rowid_alias: false,
                }
            );

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(&select) else {
                panic!("expected bound HAVING binary expression");
            };
            assert_eq!(lhs.as_ref(), &alias_expr);
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("2".into()))
            );

            assert_eq!(order_by_expr(&select, 0), &alias_expr);
        });
    }

    #[test]
    fn table_alias_hides_base_name_and_qualified_alias_resolves() {
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut good = parse_select("SELECT u.x FROM t AS u");
            ctx.bind_select(&mut good).unwrap();
            assert_column_expr(select_expr(&good, 0), 0, 0);

            let err = bind_select_error(ctx, "SELECT t.x FROM t AS u").to_string();
            assert!(
                err.contains("no such table: t") || err.contains("no such column: t.x"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn correlated_subquery_group_by_does_not_capture_outer_column_without_inner_match() {
        with_bind_context(&["CREATE TABLE t1(a, b)", "CREATE TABLE t2(x, y)"], |ctx| {
            let err = bind_select_error(
                ctx,
                "SELECT a FROM t1 WHERE EXISTS (SELECT x FROM t2 GROUP BY a)",
            )
            .to_string();
            assert!(err.contains("no such column: a"), "unexpected error: {err}");
        });
    }

    #[test]
    fn correlated_subquery_group_by_prefers_inner_column_when_present() {
        with_bind_context(&["CREATE TABLE t1(a, b)", "CREATE TABLE t3(a, x)"], |ctx| {
            let mut select =
                parse_select("SELECT a FROM t1 WHERE EXISTS (SELECT x FROM t3 GROUP BY a)");
            let bound = ctx.bind_select(&mut select).unwrap();

            let sq_id = exists_subquery_id(&select);
            let subquery = &bound.subquery_bindings[&sq_id].select;
            // t1=0, subquery_id=1, t3=2
            assert_column_expr(group_by_expr(subquery, 0), 2, 0);
        });
    }

    #[test]
    fn duplicate_aliases_are_allowed_in_order_by() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select = parse_select("SELECT x AS a, y AS a FROM t ORDER BY a");
            ctx.bind_select(&mut select).unwrap();

            assert_column_expr(order_by_expr(&select, 0), 0, 0);
        });
    }

    #[test]
    fn sqlite_compat_where_and_order_by_precedence_cases() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut where_select = parse_select("SELECT -a AS b, a, t.b FROM t WHERE b > 15");
            ctx.bind_select(&mut where_select).unwrap();
            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = where_expr(&where_select)
            else {
                panic!("expected bound WHERE binary expression");
            };
            assert_column_expr(lhs, 0, 1);
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("15".into()))
            );

            let mut order_select = parse_select("SELECT -a AS b, a, t.b FROM t ORDER BY b");
            ctx.bind_select(&mut order_select).unwrap();
            assert_eq!(
                order_by_expr(&order_select, 0),
                &ast::Expr::Unary(
                    ast::UnaryOperator::Negative,
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(1usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                )
            );
        });
    }

    #[test]
    fn sqlite_compat_group_by_prefers_source_column_over_alias() {
        // In GROUP BY, real columns take precedence over SELECT aliases
        // (matches SQLite): `b` resolves to column t.b, not alias (-a).
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT -a AS b, COUNT(*) FROM t GROUP BY b ORDER BY 1");
            ctx.bind_select(&mut select).unwrap();

            assert_column_expr(group_by_expr(&select, 0), 0, 1);
        });
    }

    #[test]
    fn order_by_subquery_can_see_select_alias_and_prefer_source_column() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            // (SELECT x) is inlined to the alias expression -a since x is only an alias
            let mut alias_visible = parse_select("SELECT a, -a AS x FROM t ORDER BY (SELECT x)");
            ctx.bind_select(&mut alias_visible).unwrap();
            assert_eq!(
                order_by_expr(&alias_visible, 0),
                &ast::Expr::Unary(
                    ast::UnaryOperator::Negative,
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                )
            );

            let mut source_preferred =
                parse_select("SELECT -a AS b, a, t.b FROM t ORDER BY (SELECT b)");
            let bound = ctx.bind_select(&mut source_preferred).unwrap();
            let sq_id = subquery_id_from_expr(order_by_expr(&source_preferred, 0));
            let order_subquery = &bound.subquery_bindings[&sq_id].select;
            assert_eq!(
                select_expr(order_subquery, 0),
                select_expr(&source_preferred, 2)
            );
        });
    }

    #[test]
    fn subqueries_in_having_and_where_can_see_outer_aliases() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut having_select = parse_select(
                "SELECT a % 2 AS g, SUM(b) AS s FROM t GROUP BY g HAVING (SELECT s) > 15 ORDER BY g",
            );
            ctx.bind_select(&mut having_select).unwrap();
            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(&having_select)
            else {
                panic!("expected bound HAVING binary expression");
            };
            // The trivial subquery `(SELECT s)` is inlined to the aggregate
            // alias expression, matching SQLite semantics (the HAVING clause
            // reads the outer aggregate value, not a nested query).
            assert_eq!(lhs.as_ref(), select_expr(&having_select, 1));
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("15".into()))
            );

            let mut nested_where = parse_select(
                "SELECT -a AS x, a \
                 FROM t \
                 WHERE EXISTS (SELECT 1 WHERE EXISTS (SELECT x WHERE x < 0))",
            );
            let bound = ctx.bind_select(&mut nested_where).unwrap();
            let outer_id = exists_subquery_id(&nested_where);
            let first_exists = &bound.subquery_bindings[&outer_id].select;
            let inner_id = exists_subquery_id(first_exists);
            let second_exists = &bound.subquery_bindings[&outer_id]
                .inner_bound
                .subquery_bindings[&inner_id]
                .select;
            assert_eq!(select_expr(second_exists, 0), select_expr(&nested_where, 0));
        });
    }

    fn window_clause(select: &ast::Select) -> &[ast::WindowDef] {
        match &select.body.select {
            ast::OneSelect::Select { window_clause, .. } => window_clause,
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    #[test]
    fn window_partition_by_and_order_by_are_bound() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select =
                parse_select("SELECT a FROM t WINDOW w AS (PARTITION BY b ORDER BY c)");
            ctx.bind_select(&mut select).unwrap();

            let defs = window_clause(&select);
            assert_eq!(defs.len(), 1);
            assert_column_expr(&defs[0].window.partition_by[0], 0, 1);
            assert_column_expr(&defs[0].window.order_by[0].expr, 0, 2);
        });
    }

    #[test]
    fn window_binds_qualified_column_refs() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select =
                parse_select("SELECT x FROM t WINDOW w AS (PARTITION BY t.y ORDER BY t.x)");
            ctx.bind_select(&mut select).unwrap();

            let defs = window_clause(&select);
            assert_column_expr(&defs[0].window.partition_by[0], 0, 1);
            assert_column_expr(&defs[0].window.order_by[0].expr, 0, 0);
        });
    }

    #[test]
    fn window_does_not_resolve_aliases() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT a AS z FROM t WINDOW w AS (PARTITION BY z)")
                .to_string();
            assert!(err.contains("no such column: z"), "unexpected error: {err}");
        });
    }

    #[test]
    fn order_by_column_number_replaces_with_result_expr() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a, b FROM t ORDER BY 2");
            ctx.bind_select(&mut select).unwrap();

            // ORDER BY 2 should resolve to column b (index 1)
            assert_column_expr(order_by_expr(&select, 0), 0, 1);
        });
    }

    #[test]
    fn group_by_column_number_replaces_with_result_expr() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a, b FROM t GROUP BY 1");
            ctx.bind_select(&mut select).unwrap();

            // GROUP BY 1 should resolve to column a (index 0)
            assert_column_expr(group_by_expr(&select, 0), 0, 0);
        });
    }

    #[test]
    fn column_number_zero_is_invalid() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT a FROM t ORDER BY 0").to_string();
            assert!(
                err.contains("1st ORDER BY term out of range - should be between 1 and 1"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn column_number_out_of_range_is_invalid() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT a FROM t ORDER BY 5").to_string();
            assert!(
                err.contains("1st ORDER BY term out of range - should be between 1 and 1"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn float_literal_in_order_by_is_not_treated_as_column_number() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t ORDER BY 1.5");
            ctx.bind_select(&mut select).unwrap();

            // 1.5 should remain as a numeric literal, not replaced
            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Literal(ast::Literal::Numeric("1.5".into()))
            );
        });
    }

    #[test]
    fn order_by_column_number_with_complex_result_expr() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a + 1, b FROM t ORDER BY 1");
            ctx.bind_select(&mut select).unwrap();

            // ORDER BY 1 should expand to the expression `a + 1` (already bound)
            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
        });
    }

    #[test]
    fn limit_clause_is_bound() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LIMIT 10");
            ctx.bind_select(&mut select).unwrap();

            let limit = select.limit.as_ref().expect("expected LIMIT clause");
            assert_eq!(
                limit.expr.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("10".into()))
            );
        });
    }

    #[test]
    fn limit_with_offset_is_bound() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LIMIT 10 OFFSET 5");
            ctx.bind_select(&mut select).unwrap();

            let limit = select.limit.as_ref().expect("expected LIMIT clause");
            assert_eq!(
                limit.expr.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("10".into()))
            );
            let offset = limit.offset.as_ref().expect("expected OFFSET");
            assert_eq!(
                offset.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("5".into()))
            );
        });
    }

    #[test]
    fn limit_double_quoted_string_becomes_literal() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LIMIT \"1\"");
            ctx.bind_select(&mut select).unwrap();

            let limit = select.limit.as_ref().expect("expected LIMIT clause");
            assert_eq!(
                limit.expr.as_ref(),
                &ast::Expr::Literal(ast::Literal::String("'1'".into()))
            );
        });
    }

    #[test]
    #[cfg(feature = "json")]
    fn function_call_star_expands_to_column_pairs() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select = parse_select("SELECT json_object(*) FROM t");
            ctx.bind_select(&mut select).unwrap();

            match select_expr(&select, 0) {
                ast::Expr::FunctionCall { name, args, .. } => {
                    assert_eq!(name.as_str(), "json_object");
                    // 2 columns × 2 (name + ref) = 4 args
                    assert_eq!(args.len(), 4);
                    assert_eq!(
                        args[0].as_ref(),
                        &ast::Expr::Literal(ast::Literal::String("'x'".into()))
                    );
                    assert_column_expr(&args[1], 0, 0);
                    assert_eq!(
                        args[2].as_ref(),
                        &ast::Expr::Literal(ast::Literal::String("'y'".into()))
                    );
                    assert_column_expr(&args[3], 0, 1);
                }
                other => panic!("expected FunctionCall, got {other:?}"),
            }
        });
    }

    #[test]
    fn function_call_star_without_expansion_stays_unchanged() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT count(*) FROM t");
            ctx.bind_select(&mut select).unwrap();

            // count(*) should remain as FunctionCallStar (not expanded)
            assert!(matches!(
                select_expr(&select, 0),
                ast::Expr::FunctionCallStar { .. }
            ));
        });
    }

    #[expect(clippy::vec_box)]
    fn values_exprs(select: &ast::Select) -> &[Vec<Box<ast::Expr>>] {
        match &select.body.select {
            ast::OneSelect::Values(rows) => rows,
            other => panic!("expected VALUES, got {other:?}"),
        }
    }

    #[test]
    fn values_double_quoted_identifier_becomes_string_literal() {
        with_bind_context(&[], |ctx| {
            let mut select = parse_select("VALUES (\"hello\")");
            ctx.bind_select(&mut select).unwrap();

            let rows = values_exprs(&select);
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][0].as_ref(),
                &ast::Expr::Literal(ast::Literal::String("'hello'".into()))
            );
        });
    }

    #[test]
    fn values_numeric_literals_are_untouched() {
        with_bind_context(&[], |ctx| {
            let mut select = parse_select("VALUES (1, 2, 3)");
            ctx.bind_select(&mut select).unwrap();

            let rows = values_exprs(&select);
            assert_eq!(rows[0].len(), 3);
            assert_eq!(
                rows[0][0].as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("1".into()))
            );
        });
    }

    #[test]
    fn values_unquoted_identifier_errors() {
        with_bind_context(&[], |ctx| {
            let err = bind_select_error(ctx, "VALUES (x)").to_string();
            assert!(err.contains("no such column: x"), "unexpected error: {err}");
        });
    }

    #[test]
    fn multiple_window_defs_are_all_bound() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select = parse_select(
                "SELECT a FROM t WINDOW w1 AS (PARTITION BY a), w2 AS (ORDER BY b, c)",
            );
            ctx.bind_select(&mut select).unwrap();

            let defs = window_clause(&select);
            assert_eq!(defs.len(), 2);
            assert_column_expr(&defs[0].window.partition_by[0], 0, 0);
            assert_eq!(defs[1].window.order_by.len(), 2);
            assert_column_expr(&defs[1].window.order_by[0].expr, 0, 1);
            assert_column_expr(&defs[1].window.order_by[1].expr, 0, 2);
        });
    }

    // ── expand_stars tests ──────────────────────────────────────────────

    fn select_columns(select: &ast::Select) -> &[ast::ResultColumn] {
        match &select.body.select {
            ast::OneSelect::Select { columns, .. } => columns,
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    #[test]
    fn expand_star_single_table() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t");
            let bound = ctx.bind_select(&mut select).unwrap();

            // The star stays unexpanded in the AST (the planner's select_star
            // expands it); its columns are visible in the bound output.
            assert!(matches!(
                select_columns(&select)[0],
                ast::ResultColumn::Star
            ));
            assert_eq!(bound.result_columns.len(), 3);
            assert_column_expr(&bound.result_columns[0].expr, 0, 0);
            assert_column_expr(&bound.result_columns[1].expr, 0, 1);
            assert_column_expr(&bound.result_columns[2].expr, 0, 2);
        });
    }

    #[test]
    fn expand_star_multiple_tables() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(x, y)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t, u");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.result_columns.len(), 4);
            // t.a, t.b, u.x, u.y
            assert_column_expr(&bound.result_columns[0].expr, 0, 0);
            assert_column_expr(&bound.result_columns[1].expr, 0, 1);
            assert_column_expr(&bound.result_columns[2].expr, 1, 0);
            assert_column_expr(&bound.result_columns[3].expr, 1, 1);
        });
    }

    #[test]
    fn expand_table_star() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(x, y)"], |ctx| {
            let mut select = parse_select("SELECT u.* FROM t, u");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.result_columns.len(), 2);
            // u.x, u.y
            assert_column_expr(&bound.result_columns[0].expr, 1, 0);
            assert_column_expr(&bound.result_columns[1].expr, 1, 1);
        });
    }

    #[test]
    fn expand_star_with_join_using_dedup() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t JOIN u USING(b)");
            let bound = ctx.bind_select(&mut select).unwrap();

            // t.a, t.b, u.c — u.b is deduped by USING
            assert_eq!(bound.result_columns.len(), 3);
            assert_column_expr(&bound.result_columns[0].expr, 0, 0);
            assert_column_expr(&bound.result_columns[1].expr, 0, 1);
            assert_column_expr(&bound.result_columns[2].expr, 1, 1);
        });
    }

    #[test]
    fn expand_star_mixed_with_explicit_columns() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT 1, *, a FROM t");
            let bound = ctx.bind_select(&mut select).unwrap();

            // literal 1, t.a, t.b, t.a
            assert_eq!(bound.result_columns.len(), 4);
            assert_eq!(
                &bound.result_columns[0].expr,
                &ast::Expr::Literal(ast::Literal::Numeric("1".into()))
            );
            assert_column_expr(&bound.result_columns[1].expr, 0, 0);
            assert_column_expr(&bound.result_columns[2].expr, 0, 1);
            assert_column_expr(&bound.result_columns[3].expr, 0, 0);
        });
    }

    #[test]
    fn expand_star_no_tables_errors() {
        with_bind_context(&[], |ctx| {
            let select = parse_select("SELECT *");
            let cols = select_columns(&select);
            // No FROM → no tables in scope → star expands to nothing
            assert_eq!(cols.len(), 1); // still Star before binding
                                       // Binding should succeed but star expands to zero columns
                                       // Actually the parser requires FROM for star, let's just test
                                       // the expand_stars produces empty
            let scope = BindScope::empty();
            let mut columns = vec![ast::ResultColumn::Star];
            ctx.expand_stars(&mut columns, &scope).unwrap();
            assert_eq!(columns.len(), 0);
        });
    }

    // ── NATURAL JOIN tests ──────────────────────────────────────────────

    fn join_constraint(select: &ast::Select) -> &Option<ast::JoinConstraint> {
        match &select.body.select {
            ast::OneSelect::Select { from, .. } => {
                let from = from.as_ref().expect("expected FROM clause");
                &from.joins[0].constraint
            }
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    #[test]
    fn natural_join_rewrites_to_using_with_common_columns() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
            ctx.bind_select(&mut select).unwrap();

            match join_constraint(&select) {
                Some(JoinConstraint::Using(cols)) => {
                    assert_eq!(cols.len(), 1);
                    assert_eq!(cols[0].as_str(), "b");
                }
                other => panic!("expected USING constraint, got {other:?}"),
            }
        });
    }

    #[test]
    fn natural_join_multiple_common_columns() {
        with_bind_context(
            &["CREATE TABLE t(a, b, c)", "CREATE TABLE u(b, c, d)"],
            |ctx| {
                let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
                ctx.bind_select(&mut select).unwrap();

                match join_constraint(&select) {
                    Some(JoinConstraint::Using(cols)) => {
                        assert_eq!(cols.len(), 2);
                        let names: Vec<&str> = cols.iter().map(|c| c.as_str()).collect();
                        assert!(names.contains(&"b"));
                        assert!(names.contains(&"c"));
                    }
                    other => panic!("expected USING constraint, got {other:?}"),
                }
            },
        );
    }

    #[test]
    fn natural_join_no_common_columns_is_cross_join() {
        // Matches SQLite: a NATURAL JOIN with no common columns degrades to a
        // cross join instead of erroring.
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
            let bound = ctx.bind_select(&mut select).unwrap();
            assert_eq!(bound.result_columns.len(), 2); // t.a, u.b — no dedup, no constraint
        });
    }

    #[test]
    fn natural_join_with_on_clause_errors() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let err =
                bind_select_error(ctx, "SELECT * FROM t NATURAL JOIN u ON t.b = u.b").to_string();
            assert!(
                err.contains("a NATURAL join may not have an ON or USING clause"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn natural_join_star_deduplicates_common_columns() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
            let bound = ctx.bind_select(&mut select).unwrap();

            // t.a, t.b, u.c — u.b is deduped by USING(b)
            assert_eq!(bound.result_columns.len(), 3);
            assert_column_expr(&bound.result_columns[0].expr, 0, 0); // t.a
            assert_column_expr(&bound.result_columns[1].expr, 0, 1); // t.b
            assert_column_expr(&bound.result_columns[2].expr, 1, 1); // u.c
        });
    }

    #[test]
    fn natural_join_rewrites_constraint_to_using() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t NATURAL JOIN u");
            let bound = ctx.bind_select(&mut select).unwrap();

            // SELECT-list usage is tracked by the binder.
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 0))); // t.a from SELECT

            // The NATURAL constraint is rewritten to USING(b); the equality
            // predicate synthesis (and its column-usage marking) happens later
            // in fold_join_constraints, mirroring parse_join.
            let ast::OneSelect::Select { from, .. } = &select.body.select else {
                panic!("expected SELECT core");
            };
            let from = from.as_ref().expect("expected FROM clause");
            match &from.joins[0].constraint {
                Some(ast::JoinConstraint::Using(cols)) => {
                    assert_eq!(cols.len(), 1);
                    assert!(cols[0].as_str().eq_ignore_ascii_case("b"));
                }
                other => panic!("expected USING constraint, got {other:?}"),
            }
        });
    }

    #[test]
    fn bind_cte_multi_reference_produces_separate_scope_tables() {
        // When the same CTE is referenced twice (e.g. FROM cte t1 JOIN cte t2),
        // into_table_references must produce two JoinedTables — not fail because
        // the CTE was consumed by the first reference.
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut select =
                parse_select("WITH c AS (SELECT x FROM t) SELECT t1.x, t2.x FROM c t1, c t2");
            let bound = ctx.bind_select(&mut select).unwrap();

            // The scope should have two tables (c t1 and c t2) with distinct internal_ids.
            assert_eq!(bound.main_scope.tables.len(), 2);
            assert_ne!(
                bound.main_scope.tables[0].internal_id,
                bound.main_scope.tables[1].internal_id
            );
            assert_eq!(bound.main_scope.tables[0].identifier, "t1");
            assert_eq!(bound.main_scope.tables[1].identifier, "t2");
        });
    }

    #[test]
    fn bind_cte_definitions_preserve_definition_order() {
        // referenced_cte_indices are offsets into the cte_definitions vec.
        // If cte_definitions were collected in arbitrary HashMap iteration order,
        // the indices would point to the wrong CTEs, causing infinite recursion
        // during planning.
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut select = parse_select(
                "WITH a AS (SELECT x FROM t), \
                      b AS (SELECT x FROM a), \
                      c AS (SELECT x FROM a) \
                 SELECT * FROM c",
            );
            let bound = ctx.bind_select(&mut select).unwrap();

            // Validate that referenced_cte_indices actually point to the right CTEs.
            // b references a, and c references a. Regardless of iteration order,
            // the index stored must resolve to "a" in the cte_definitions vec.
            for (name, entry) in &bound.cte_definitions {
                if name == "b" || name == "c" {
                    assert_eq!(
                        entry.referenced_cte_indices.len(),
                        1,
                        "CTE '{name}' should reference exactly one sibling"
                    );
                    let ref_idx = entry.referenced_cte_indices[0];
                    assert_eq!(
                        bound.cte_definitions[ref_idx].0, "a",
                        "CTE '{name}' references index {ref_idx} which should be 'a', \
                         but found '{}' — cte_definitions is not in definition order",
                        bound.cte_definitions[ref_idx].0
                    );
                }
            }
        });
    }
}
