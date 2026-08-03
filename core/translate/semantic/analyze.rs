use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast;

use crate::{
    numeric::Numeric, schema::Type, util::parse_numeric_literal, LimboError, Result, Value,
    MAIN_DB_ID,
};

use super::{
    context::SemanticContext,
    expr::ExprPolicy,
    hir::{
        BoundSchemaProgram, CatalogObjectId, Cte, DatabaseId, DatabaseSnapshot, Expr, HirDocument,
        HirRoot, Output, OutputId, OutputNameKind, Query, QueryBlock, QueryBlockBody, QueryBlockId,
        QueryId, QueryRoot, Source, SourceId, TypeFact,
    },
    scope::{ExpandedColumn, Scope},
    AnalyzeInput,
};

pub(crate) fn analyze(
    context: &SemanticContext<'_>,
    input: AnalyzeInput<'_>,
) -> Result<HirDocument> {
    let mut analyzer = Analyzer::new(context);
    let root = match input {
        AnalyzeInput::Statement(ast::Stmt::Select(select)) => {
            let query = analyzer.analyze_select(select)?;
            HirRoot::Query(QueryRoot {
                query,
                trigger: None,
            })
        }
        AnalyzeInput::Statement(_) => {
            return Err(LimboError::ParseError(
                "semantic analysis accepts SELECT statements".to_string(),
            ));
        }
    };
    let document = analyzer.finish(root)?;
    document.validate().map_err(|error| {
        LimboError::InternalError(format!("semantic analysis produced invalid HIR: {error}"))
    })?;
    Ok(document)
}

pub(super) struct Analyzer<'context, 'catalog> {
    context: &'context SemanticContext<'catalog>,
    queries: Vec<Option<Query>>,
    sources: Vec<Option<Source>>,
    ctes: Vec<Option<Cte>>,
    schema_programs: Vec<Option<BoundSchemaProgram>>,
    catalog_ids: HashMap<CatalogIdentity, CatalogObjectId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum CatalogObjectKind {
    Table,
    Collation,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CatalogIdentity {
    database: Option<DatabaseId>,
    kind: CatalogObjectKind,
    name: String,
}

impl<'context, 'catalog> Analyzer<'context, 'catalog> {
    pub(super) fn new(context: &'context SemanticContext<'catalog>) -> Self {
        Self {
            context,
            queries: Vec::new(),
            sources: Vec::new(),
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            catalog_ids: HashMap::default(),
        }
    }

    pub(super) const fn context(&self) -> &SemanticContext<'catalog> {
        self.context
    }

    fn reserve_query(&mut self) -> QueryId {
        let id = QueryId::new(self.queries.len());
        self.queries.push(None);
        id
    }

    pub(super) fn reserve_source(&mut self) -> SourceId {
        let id = SourceId::new(self.sources.len());
        self.sources.push(None);
        id
    }

    pub(super) fn insert_source(&mut self, id: SourceId, source: Source) -> Result<()> {
        if source.id != id {
            return Err(LimboError::InternalError(format!(
                "source {} was inserted into slot {}",
                source.id, id
            )));
        }
        Self::insert_reserved(&mut self.sources, id.index(), source, "source")
    }

    pub(super) fn source(&self, id: SourceId) -> Option<&Source> {
        self.sources.get(id.index())?.as_ref()
    }

    pub(super) fn catalog_object_id(
        &mut self,
        database: Option<DatabaseId>,
        kind: CatalogObjectKind,
        name: impl Into<String>,
    ) -> CatalogObjectId {
        let identity = CatalogIdentity {
            database,
            kind,
            name: name.into(),
        };
        if let Some(id) = self.catalog_ids.get(&identity) {
            return *id;
        }
        let id = CatalogObjectId::new(self.catalog_ids.len() as u64);
        self.catalog_ids.insert(identity, id);
        id
    }

    fn insert_query(&mut self, id: QueryId, query: Query) -> Result<()> {
        if query.id != id {
            return Err(LimboError::InternalError(format!(
                "query {} was inserted into slot {}",
                query.id, id
            )));
        }
        Self::insert_reserved(&mut self.queries, id.index(), query, "query")
    }

    fn insert_reserved<T>(
        arena: &mut [Option<T>],
        index: usize,
        value: T,
        kind: &str,
    ) -> Result<()> {
        let Some(slot) = arena.get_mut(index) else {
            return Err(LimboError::InternalError(format!(
                "{kind} slot {index} was not reserved"
            )));
        };
        if slot.is_some() {
            return Err(LimboError::InternalError(format!(
                "{kind} slot {index} was filled twice"
            )));
        }
        *slot = Some(value);
        Ok(())
    }

    fn finish_arena<T>(arena: Vec<Option<T>>, kind: &str) -> Result<Vec<T>> {
        arena
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                value.ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "reserved {kind} slot {index} was not filled"
                    ))
                })
            })
            .collect()
    }

    fn finish(self, root: HirRoot) -> Result<HirDocument> {
        Ok(HirDocument {
            snapshot: self.context.snapshot(),
            databases: vec![DatabaseSnapshot {
                database: DatabaseId::new(MAIN_DB_ID),
                schema_version: self.context.main_schema().schema_version,
            }],
            root,
            queries: Self::finish_arena(self.queries, "query")?,
            sources: Self::finish_arena(self.sources, "source")?,
            ctes: Self::finish_arena(self.ctes, "CTE")?,
            schema_programs: Self::finish_arena(self.schema_programs, "schema program")?,
            cdc: None,
        })
    }

    fn analyze_select(&mut self, select: &ast::Select) -> Result<QueryId> {
        if select.with.is_some()
            || !select.body.compounds.is_empty()
            || !select.order_by.is_empty()
            || select.limit.is_some()
        {
            return unsupported_select();
        }

        let ast::OneSelect::Select {
            distinctness,
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
        } = &select.body.select
        else {
            return unsupported_select();
        };
        if where_clause.is_some() || group_by.is_some() || !window_clause.is_empty() {
            return unsupported_select();
        }
        if from.is_none()
            && columns
                .iter()
                .any(|column| matches!(column, ast::ResultColumn::Star))
        {
            crate::bail_parse_error!("no tables specified");
        }

        let query_id = self.reserve_query();
        let block_id = QueryBlockId::new(query_id, 0);
        let (from, scope) = match from {
            Some(from) => {
                let (from, scope) =
                    self.analyze_from_clause(from, super::hir::SourceOwner::QueryBlock(block_id))?;
                (Some(from), scope)
            }
            None => (None, Scope::default()),
        };
        let outputs = self.analyze_outputs(block_id, columns, &scope)?;
        let output_ids = outputs.iter().map(|output| output.id).collect();

        self.insert_query(
            query_id,
            Query {
                id: query_id,
                parent: None,
                captures: Vec::new(),
                reachable_ctes: Vec::new(),
                blocks: vec![QueryBlock {
                    id: block_id,
                    from,
                    outputs,
                    aggregate_count: 0,
                    window_function_count: 0,
                    body: QueryBlockBody::Select {
                        distinctness: *distinctness,
                        filter: None,
                        grouping: None,
                        windows: Vec::new(),
                    },
                }],
                first: block_id,
                compounds: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                output: output_ids,
            },
        )?;

        Ok(query_id)
    }

    fn analyze_outputs(
        &mut self,
        block: QueryBlockId,
        columns: &[ast::ResultColumn],
        scope: &Scope,
    ) -> Result<Vec<Output>> {
        let mut outputs = Vec::with_capacity(columns.len());
        for column in columns {
            match column {
                ast::ResultColumn::Expr(_, _) => {
                    outputs.push(self.analyze_output(block, outputs.len(), column, scope)?);
                }
                ast::ResultColumn::Star => {
                    let expanded = scope.expand_star()?;
                    self.append_star_outputs(block, &mut outputs, expanded, scope)?;
                }
                ast::ResultColumn::TableStar(table) => {
                    let expanded = scope.expand_table_star(table.as_str())?;
                    self.append_star_outputs(block, &mut outputs, expanded, scope)?;
                }
            }
        }
        Ok(outputs)
    }

    fn append_star_outputs(
        &self,
        block: QueryBlockId,
        outputs: &mut Vec<Output>,
        expanded: Vec<ExpandedColumn>,
        scope: &Scope,
    ) -> Result<()> {
        for column in expanded {
            let resolved = self.resolve_atomic_expr(column.resolved.expr, scope)?;
            let (collation, collation_is_explicit) = resolved.collation.into_output();
            outputs.push(Output {
                id: OutputId::query(block, outputs.len()),
                name: column.name,
                expr: resolved.expr,
                type_fact: resolved.type_fact,
                affinity: resolved.affinity,
                schema_affinity: resolved.affinity,
                has_affinity: resolved.has_affinity,
                collation,
                collation_is_explicit,
                name_kind: OutputNameKind::StarExpansion,
            });
        }
        Ok(())
    }

    fn analyze_output(
        &mut self,
        block: QueryBlockId,
        index: usize,
        column: &ast::ResultColumn,
        scope: &Scope,
    ) -> Result<Output> {
        let ast::ResultColumn::Expr(expression, alias) = column else {
            return unsupported_select();
        };
        let syntax = expression;
        let policy = ExprPolicy::select(self.context.dqs_dml());
        let resolved = self.analyze_resolved_expr(syntax, scope, policy)?;
        let (collation, collation_is_explicit) = resolved.collation.into_output();
        let (name, name_kind) = match alias {
            Some(alias) if alias.is_explicit() => (
                alias.name().as_str().to_string(),
                OutputNameKind::ExplicitAlias,
            ),
            Some(ast::As::ImplicitColumnName(name)) => {
                (name.as_str().to_string(), OutputNameKind::Inferred)
            }
            None => (syntax.to_string(), OutputNameKind::Inferred),
            Some(_) => unreachable!("all explicit aliases were handled"),
        };
        Ok(Output {
            id: OutputId::query(block, index),
            name,
            expr: resolved.expr,
            type_fact: resolved.type_fact,
            affinity: resolved.affinity,
            schema_affinity: resolved.affinity,
            has_affinity: resolved.has_affinity,
            collation,
            collation_is_explicit,
            name_kind,
        })
    }
}

pub(super) fn literal_type_fact(literal: &ast::Literal) -> Result<TypeFact> {
    let storage = match literal {
        ast::Literal::Numeric(value) => match parse_numeric_literal(value)? {
            Value::Numeric(Numeric::Integer(_)) => Type::Integer,
            Value::Numeric(Numeric::Float(_)) => Type::Real,
            _ => unreachable!("numeric literal parser returned a non-numeric value"),
        },
        ast::Literal::String(_)
        | ast::Literal::CurrentDate
        | ast::Literal::CurrentTime
        | ast::Literal::CurrentTimestamp => Type::Text,
        ast::Literal::Blob(_) => Type::Blob,
        ast::Literal::Null => Type::Null,
        ast::Literal::True | ast::Literal::False => Type::Integer,
        ast::Literal::Keyword(keyword) => {
            return Err(LimboError::ParseError(format!(
                "unresolved keyword literal: {keyword}"
            )));
        }
    };
    Ok(TypeFact::known(storage))
}

pub(super) fn unsupported_select<T>() -> Result<T> {
    Err(LimboError::ParseError(
        "semantic analysis accepts source-free literal SELECT statements".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use turso_parser::{ast, parser::Parser};

    use crate::{
        dialect::SqliteDialect,
        schema::{BTreeTable, Schema, Type},
        sync::Arc,
        SymbolTable,
    };

    use super::*;
    use crate::translate::semantic::{
        context::DoubleQuotedDml,
        hir::{HirRoot, OutputNameKind, SourceKind, SourceOwner},
    };

    fn parse_statement(sql: &str) -> ast::Stmt {
        let command = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains a statement");
        let ast::Cmd::Stmt(statement) = command else {
            panic!("SQL contains a statement");
        };
        statement
    }

    fn analyze_sql(sql: &str) -> Result<HirDocument> {
        let schema = Schema::new();
        analyze_sql_with_schema(&schema, sql)
    }

    fn analyze_sql_with_schema(schema: &Schema, sql: &str) -> Result<HirDocument> {
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        let statement = parse_statement(sql);
        analyze(&context, AnalyzeInput::Statement(&statement))
    }

    fn schema_with_items() -> Schema {
        let mut schema = Schema::new();
        let table = Arc::new(
            BTreeTable::from_sql(
                "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT COLLATE NOCASE, score REAL)",
                2,
            )
            .expect("fixed table schema parses"),
        );
        schema
            .add_btree_table(table)
            .expect("fixed table name is unique");
        schema
    }

    #[test]
    fn source_free_literals_become_closed_hir() {
        let document = analyze_sql("SELECT 1, 1.5 AS real_value, 'text', NULL, TRUE")
            .expect("literal SELECT has valid SQL meaning");
        document.validate().expect("analyzer returns closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert!(block.from.is_none());
        assert_eq!(block.outputs.len(), 5);
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Integer));
        assert_eq!(block.outputs[1].type_fact.storage, Some(Type::Real));
        assert_eq!(block.outputs[1].name, "real_value");
        assert_eq!(block.outputs[1].name_kind, OutputNameKind::ExplicitAlias);
        assert_eq!(block.outputs[2].type_fact.storage, Some(Type::Text));
        assert_eq!(block.outputs[3].type_fact.storage, Some(Type::Null));
        assert_eq!(block.outputs[4].type_fact.storage, Some(Type::Integer));
        assert!(block.outputs.iter().all(|output| !output.has_affinity));
    }

    #[test]
    fn unresolved_names_do_not_enter_hir() {
        let error = analyze_sql("SELECT missing_name")
            .expect_err("unresolved name must fail semantic analysis");
        assert_eq!(
            error.to_string(),
            "Parse error: no such column: missing_name"
        );
    }

    #[test]
    fn unresolved_double_quoted_names_follow_dqs_setting() {
        let document = analyze_sql("SELECT \"missing name\"")
            .expect("enabled DQS converts unresolved name to text");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        assert!(matches!(
            &output.expr,
            Expr::Literal(ast::Literal::String(value)) if value == "'missing name'"
        ));
        assert_eq!(output.type_fact.storage, Some(Type::Text));

        let schema = Schema::new();
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            &schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        )
        .with_dqs_dml(DoubleQuotedDml::Disabled);
        let statement = parse_statement("SELECT \"missing name\"");
        let error = analyze(&context, AnalyzeInput::Statement(&statement))
            .expect_err("disabled DQS keeps unresolved name error");
        assert_eq!(
            error.to_string(),
            "Parse error: no such column: missing name"
        );
    }

    #[test]
    fn plain_table_select_becomes_closed_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value, items.score, main.items.id, rowid FROM items",
        )
        .expect("plain table SELECT has valid SQL meaning");
        document.validate().expect("analyzer returns closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        let block = &query.blocks[0];
        let from = block.from.as_ref().expect("FROM is preserved");
        let source = document.source(from.first).expect("source exists");
        assert_eq!(source.owner, SourceOwner::QueryBlock(block.id));
        assert_eq!(source.name, "items");
        assert!(matches!(&source.kind, SourceKind::Table(_)));
        assert_eq!(source.columns.len(), 3);

        assert!(matches!(
            &block.outputs[0].expr,
            Expr::Column(reference) if reference.source == source.id && reference.column == 1
        ));
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Text));
        assert_eq!(
            block.outputs[0].affinity,
            crate::vdbe::affinity::Affinity::Text
        );
        assert!(block.outputs[0].has_affinity);
        assert_eq!(
            block.outputs[0]
                .collation
                .as_ref()
                .expect("declared collation is preserved")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert!(!block.outputs[0].collation_is_explicit);
        assert!(matches!(
            &block.outputs[1].expr,
            Expr::Column(reference) if reference.source == source.id && reference.column == 2
        ));
        assert!(matches!(
            &block.outputs[2].expr,
            Expr::Column(reference) if reference.source == source.id && reference.column == 0
        ));
        assert!(matches!(&block.outputs[3].expr, Expr::RowId(id) if *id == source.id));
        assert_eq!(block.outputs[3].type_fact.storage, Some(Type::Integer));
    }

    #[test]
    fn stars_become_ordered_hir_outputs() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(&schema, "SELECT *, score FROM items")
            .expect("star expands against the FROM scope");
        document
            .validate()
            .expect("star expansion produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(
            block
                .outputs
                .iter()
                .map(|output| output.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "value", "score", "score"]
        );
        for (index, output) in block.outputs.iter().enumerate() {
            assert_eq!(output.id, OutputId::query(block.id, index));
        }
        assert!(block.outputs[..3]
            .iter()
            .all(|output| output.name_kind == OutputNameKind::StarExpansion));
        assert_eq!(block.outputs[3].name_kind, OutputNameKind::Inferred);
        assert_eq!(
            block.outputs[1]
                .collation
                .as_ref()
                .expect("star preserves declared collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert!(!block.outputs[1].collation_is_explicit);
    }

    #[test]
    fn basic_expressions_become_closed_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT -id, NOT value, score + 1.5, value || 'x', \
             value = 'x', value IS NULL, value NOTNULL FROM items",
        )
        .expect("basic expressions have valid SQL meaning");
        document
            .validate()
            .expect("basic expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 7);
        assert!(matches!(
            outputs[0].expr,
            Expr::Unary {
                operator: ast::UnaryOperator::Negative,
                ..
            }
        ));
        assert_eq!(outputs[0].type_fact.storage, Some(Type::Integer));
        assert!(matches!(
            outputs[1].expr,
            Expr::Unary {
                operator: ast::UnaryOperator::Not,
                ..
            }
        ));
        assert_eq!(outputs[1].type_fact.storage, Some(Type::Integer));
        assert_eq!(outputs[2].type_fact.storage, Some(Type::Real));
        assert_eq!(outputs[3].type_fact.storage, Some(Type::Text));
        assert!(outputs.iter().all(|output| !output.has_affinity));

        let Expr::Binary {
            operator: ast::Operator::Equals,
            comparison: Some(comparison),
            ..
        } = &outputs[4].expr
        else {
            panic!("comparison becomes binary HIR with frozen rules");
        };
        assert_eq!(comparison.components.len(), 1);
        assert_eq!(
            comparison.components[0].affinity,
            crate::vdbe::affinity::Affinity::Text
        );
        assert_eq!(
            comparison.components[0]
                .collation
                .as_ref()
                .expect("declared collation reaches comparison")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert!(matches!(outputs[5].expr, Expr::IsNull(_)));
        assert!(matches!(outputs[6].expr, Expr::NotNull(_)));
        assert_eq!(outputs[5].type_fact.storage, Some(Type::Integer));
        assert_eq!(outputs[6].type_fact.storage, Some(Type::Integer));
    }

    #[test]
    fn explicit_collations_override_inherited_collations() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value COLLATE RTRIM, \
             value = ('x' || ('y' COLLATE RTRIM)), \
             (('x' COLLATE RTRIM) || 'y') = value FROM items",
        )
        .expect("explicit collations bind into HIR");
        document
            .validate()
            .expect("collated expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;

        let Expr::Collate { collation, .. } = &outputs[0].expr else {
            panic!("COLLATE becomes resolved HIR");
        };
        assert_eq!(
            collation.value(),
            &crate::translate::collate::CollationSeq::Rtrim
        );
        assert_eq!(outputs[0].affinity, crate::vdbe::affinity::Affinity::Text);
        assert!(outputs[0].has_affinity);
        assert!(outputs[0].collation_is_explicit);

        for output in &outputs[1..] {
            let Expr::Binary {
                comparison: Some(comparison),
                ..
            } = &output.expr
            else {
                panic!("comparison rules are frozen in binary HIR");
            };
            assert_eq!(
                comparison.components[0]
                    .collation
                    .as_ref()
                    .expect("explicit collation reaches comparison")
                    .value(),
                &crate::translate::collate::CollationSeq::Rtrim
            );
            assert!(output.collation_is_explicit);
        }
    }

    #[test]
    fn table_star_uses_alias_visibility() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(&schema, "SELECT i.* FROM items AS i")
            .expect("table alias expands star");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.outputs.len(), 3);
        assert!(block
            .outputs
            .iter()
            .all(|output| output.name_kind == OutputNameKind::StarExpansion));

        let hidden_name = analyze_sql_with_schema(&schema, "SELECT items.* FROM items AS i")
            .expect_err("alias hides the original table name");
        assert_eq!(hidden_name.to_string(), "Parse error: no such table: items");

        let no_from = analyze_sql("SELECT *").expect_err("star requires a FROM source");
        assert_eq!(no_from.to_string(), "Parse error: no tables specified");
    }

    #[test]
    fn table_alias_controls_hir_name_visibility() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(&schema, "SELECT i.value FROM items AS i")
            .expect("alias-qualified column resolves");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source = document
            .source(block.from.as_ref().expect("FROM exists").first)
            .expect("source exists");
        assert_eq!(source.alias.as_deref(), Some("i"));

        let error = analyze_sql_with_schema(&schema, "SELECT main.items.value FROM items AS i")
            .expect_err("alias hides database-qualified table name");
        assert_eq!(
            error.to_string(),
            "Parse error: no such column: main.items.value"
        );
    }

    #[test]
    fn missing_table_fails_before_hir_is_built() {
        let error = analyze_sql("SELECT value FROM absent").expect_err("table must exist");
        assert_eq!(error.to_string(), "Parse error: no such table: absent");
    }
}
