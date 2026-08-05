use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast;

use crate::{
    numeric::Numeric, schema::Type, util::parse_numeric_literal, LimboError, Result, Value,
    MAIN_DB_ID,
};

use super::{
    context::SemanticContext,
    expr::{ExprPolicy, QueryFunctionState},
    hir::{
        BoundSchemaProgram, CatalogObjectId, CompoundArm, Cte, DatabaseId, DatabaseSnapshot, Expr,
        HirDocument, HirRoot, Output, OutputId, OutputNameKind, Query, QueryBlock, QueryBlockBody,
        QueryBlockId, QueryId, QueryRoot, SchemaProgramId, Source, SourceId, TypeFact,
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
    schema_programs_in_progress: HashSet<CatalogObjectId>,
    catalog_ids: HashMap<CatalogIdentity, CatalogObjectId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum CatalogObjectKind {
    Table,
    Sequence,
    Collation,
    Type,
    Function { argument_count: usize },
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
            schema_programs_in_progress: HashSet::default(),
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

    pub(super) fn reserve_schema_program(&mut self) -> SchemaProgramId {
        let id = SchemaProgramId::new(self.schema_programs.len());
        self.schema_programs.push(None);
        id
    }

    pub(super) fn insert_schema_program(
        &mut self,
        id: SchemaProgramId,
        program: BoundSchemaProgram,
    ) -> Result<()> {
        Self::insert_reserved(
            &mut self.schema_programs,
            id.index(),
            program,
            "schema program",
        )
    }

    pub(super) fn enter_schema_program_binding(&mut self, definition: CatalogObjectId) -> bool {
        self.schema_programs_in_progress.insert(definition)
    }

    pub(super) fn leave_schema_program_binding(&mut self, definition: CatalogObjectId) {
        assert!(
            self.schema_programs_in_progress.remove(&definition),
            "schema program binding must be active before it finishes"
        );
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
        if select.with.is_some() || !select.order_by.is_empty() || select.limit.is_some() {
            return unsupported_select();
        }

        let query_id = self.reserve_query();
        let mut blocks = Vec::with_capacity(select.body.compounds.len() + 1);
        blocks.push(self.analyze_select_block(&select.body.select, query_id, 0)?);
        for (index, compound) in select.body.compounds.iter().enumerate() {
            blocks.push(self.analyze_select_block(&compound.select, query_id, index + 1)?);
        }

        if let Some(rightmost) = blocks.last() {
            let rightmost_width = rightmost.outputs.len();
            if let Some((_, compound)) = blocks[..blocks.len() - 1]
                .iter()
                .zip(&select.body.compounds)
                .find(|(block, _)| block.outputs.len() != rightmost_width)
            {
                crate::bail_parse_error!(
                    "SELECTs to the left and right of {} do not have the same number of result columns",
                    compound.operator
                );
            }
        }

        let first = blocks[0].id;
        let output = blocks[0].outputs.iter().map(|output| output.id).collect();
        let compounds = select
            .body
            .compounds
            .iter()
            .zip(blocks.iter().skip(1))
            .map(|(compound, block)| CompoundArm {
                operator: compound.operator,
                block: block.id,
            })
            .collect();

        self.insert_query(
            query_id,
            Query {
                id: query_id,
                parent: None,
                captures: Vec::new(),
                reachable_ctes: Vec::new(),
                blocks,
                first,
                compounds,
                order_by: Vec::new(),
                limit: None,
                output,
            },
        )?;

        Ok(query_id)
    }

    fn analyze_select_block(
        &mut self,
        select: &ast::OneSelect,
        query: QueryId,
        index: usize,
    ) -> Result<QueryBlock> {
        let ast::OneSelect::Select {
            distinctness,
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
        } = select
        else {
            return unsupported_select();
        };
        if where_clause.is_some() || group_by.is_some() {
            return unsupported_select();
        }
        if from.is_none()
            && columns
                .iter()
                .any(|column| matches!(column, ast::ResultColumn::Star))
        {
            crate::bail_parse_error!("no tables specified");
        }

        let block_id = QueryBlockId::new(query, index);
        let (from, scope) = match from {
            Some(from) => {
                let (from, scope) =
                    self.analyze_from_clause(from, super::hir::SourceOwner::QueryBlock(block_id))?;
                (Some(from), scope)
            }
            None => (None, Scope::default()),
        };
        let mut functions = QueryFunctionState::new(block_id);
        self.analyze_named_windows(
            window_clause,
            &scope,
            ExprPolicy::select(self.context.dqs_dml()),
            &mut functions,
        )?;
        let outputs = self.analyze_outputs(block_id, columns, &scope, &mut functions)?;
        let aggregate_count = functions.aggregate_count();
        let window_function_count = functions.window_function_count();
        let windows = functions.take_windows();

        Ok(QueryBlock {
            id: block_id,
            from,
            outputs,
            aggregate_count,
            window_function_count,
            windows,
            body: QueryBlockBody::Select {
                distinctness: *distinctness,
                filter: None,
                grouping: None,
            },
        })
    }

    fn analyze_outputs(
        &mut self,
        block: QueryBlockId,
        columns: &[ast::ResultColumn],
        scope: &Scope,
        functions: &mut QueryFunctionState,
    ) -> Result<Vec<Output>> {
        let mut outputs = Vec::with_capacity(columns.len());
        for column in columns {
            match column {
                ast::ResultColumn::Expr(_, _) => {
                    outputs.push(self.analyze_output(
                        block,
                        outputs.len(),
                        column,
                        scope,
                        functions,
                    )?);
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
        functions: &mut QueryFunctionState,
    ) -> Result<Output> {
        let ast::ResultColumn::Expr(expression, alias) = column else {
            return unsupported_select();
        };
        let syntax = expression;
        let policy = ExprPolicy::select(self.context.dqs_dml());
        let resolved = self.analyze_query_expr(syntax, scope, policy, functions)?;
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
        schema::{BTreeTable, Schema, Sequence, Type},
        sync::Arc,
        SymbolTable,
    };

    use super::*;
    use crate::translate::semantic::{
        context::DoubleQuotedDml,
        hir::{
            CustomTypeOperation, FunctionEvaluation, FunctionOperation, HirRoot, JoinConstraint,
            JoinKind, MergedColumnValue, OutputNameKind, SourceKind, SourceOwner,
        },
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

    fn schema_with_join_tables() -> Schema {
        let mut schema = schema_with_items();
        for (sql, root_page) in [
            ("CREATE TABLE categories(id INTEGER, label TEXT)", 3),
            ("CREATE TABLE tags(item_id INTEGER, note TEXT)", 4),
            (
                "CREATE TABLE extras(item_id INTEGER, flag INTEGER, id INTEGER)",
                5,
            ),
            ("CREATE TABLE codes(value INTEGER, label TEXT)", 6),
        ] {
            let table = Arc::new(
                BTreeTable::from_sql(sql, root_page).expect("fixed join table schema parses"),
            );
            schema
                .add_btree_table(table)
                .expect("fixed join table name is unique");
        }
        schema
    }

    fn schema_with_sequence(name: &str) -> Schema {
        let mut schema = Schema::new();
        let normalized = crate::util::normalize_ident(name);
        let sequence = Sequence::new(normalized.clone(), None, None, None, None, false)
            .expect("fixed sequence descriptor is valid");
        schema
            .sequences
            .insert(normalized.clone(), Arc::new(sequence));
        let table = BTreeTable::from_sql(
            &crate::translate::sequence::sequence_backing_table_sql(&normalized),
            2,
        )
        .expect("sequence backing table parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("sequence backing table name is unique");
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
    fn parameters_keep_parser_assigned_indexes_and_names() {
        let document =
            analyze_sql("SELECT ?, ?3, :named, :named, @other").expect("parameters bind into HIR");
        document
            .validate()
            .expect("parameter expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        let expected = [
            (1, None),
            (3, None),
            (4, Some(":named")),
            (4, Some(":named")),
            (5, Some("@other")),
        ];

        for (output, (index, name)) in outputs.iter().zip(expected) {
            let Expr::Parameter(parameter) = &output.expr else {
                panic!("parser variable becomes HIR parameter");
            };
            assert_eq!(parameter.index.get(), index);
            assert_eq!(parameter.name.as_deref(), name);
            assert!(parameter.type_fact.storage.is_none());
            assert!(parameter.type_fact.declared.is_none());
            assert!(!output.has_affinity);
            assert!(output.collation.is_none());
            assert!(!output.collation_is_explicit);
        }
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
    fn compound_selects_become_ordered_query_blocks() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT id FROM items \
             UNION ALL SELECT id FROM categories \
             UNION SELECT item_id FROM tags \
             INTERSECT SELECT flag FROM extras \
             EXCEPT SELECT value FROM codes",
        )
        .expect("compound SELECT binds into HIR");
        document
            .validate()
            .expect("compound SELECT produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        assert_eq!(query.blocks.len(), 5);
        assert_eq!(query.first, query.blocks[0].id);
        assert_eq!(query.output, vec![query.blocks[0].outputs[0].id]);

        let expected_operators = [
            ast::CompoundOperator::UnionAll,
            ast::CompoundOperator::Union,
            ast::CompoundOperator::Intersect,
            ast::CompoundOperator::Except,
        ];
        for ((arm, block), operator) in query
            .compounds
            .iter()
            .zip(query.blocks.iter().skip(1))
            .zip(expected_operators)
        {
            assert_eq!(arm.operator, operator);
            assert_eq!(arm.block, block.id);
        }

        let expected_tables = ["items", "categories", "tags", "extras", "codes"];
        for (block, table) in query.blocks.iter().zip(expected_tables) {
            let source = document
                .source(block.from.as_ref().expect("arm has FROM").first)
                .expect("arm source exists");
            assert_eq!(source.name, table);
            assert_eq!(source.owner, SourceOwner::QueryBlock(block.id));
            assert_eq!(block.outputs.len(), 1);
        }
    }

    #[test]
    fn compound_aggregate_identities_belong_to_each_arm() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT count(*) FROM items UNION ALL SELECT count(*) FROM categories",
        )
        .expect("aggregate compound SELECT binds into HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        for block in &query.blocks {
            assert_eq!(block.aggregate_count, 1);
            let Expr::Function(function) = &block.outputs[0].expr else {
                panic!("count becomes a resolved function");
            };
            assert!(matches!(
                function.evaluation,
                FunctionEvaluation::Aggregate { id, .. }
                    if id == crate::translate::semantic::hir::AggregateId::new(block.id, 0)
            ));
        }
    }

    #[test]
    fn compound_selects_keep_width_diagnostics() {
        let error = analyze_sql("SELECT 1, 2 UNION ALL SELECT 3 UNION SELECT 4, 5")
            .expect_err("different compound widths fail semantic analysis");
        assert_eq!(
            error.to_string(),
            "Parse error: SELECTs to the left and right of UNION do not have the same number of result columns"
        );
    }

    #[test]
    fn basic_joins_keep_source_order_kinds_and_closed_on_expressions() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT i.value, c.label, t.note, e.flag \
             FROM items AS i, categories AS c \
             JOIN tags AS t ON e.item_id = i.id \
             CROSS JOIN extras AS e",
        )
        .expect("basic joins bind into HIR");
        document.validate().expect("basic joins produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let from = block.from.as_ref().expect("joined query has FROM");
        assert_eq!(from.joins.len(), 3);
        assert_eq!(from.joins[0].kind, JoinKind::Comma);
        assert_eq!(from.joins[1].kind, JoinKind::Inner);
        assert_eq!(from.joins[2].kind, JoinKind::Cross);

        let sources = [
            from.first,
            from.joins[0].right,
            from.joins[1].right,
            from.joins[2].right,
        ];
        let aliases: Vec<_> = sources
            .iter()
            .map(|source| {
                document
                    .source(*source)
                    .expect("join source exists")
                    .alias
                    .as_deref()
            })
            .collect();
        assert_eq!(aliases, [Some("i"), Some("c"), Some("t"), Some("e")]);

        let JoinConstraint::On(Expr::Binary { lhs, rhs, .. }) = &from.joins[1].constraint else {
            panic!("JOIN ON becomes a resolved HIR expression");
        };
        assert!(matches!(
            lhs.as_ref(),
            Expr::Column(reference) if reference.source == sources[3] && reference.column == 0
        ));
        assert!(matches!(
            rhs.as_ref(),
            Expr::Column(reference) if reference.source == sources[0] && reference.column == 0
        ));

        for (output, source) in block.outputs.iter().zip(sources) {
            assert!(matches!(
                output.expr,
                Expr::Column(reference) if reference.source == source && reference.column == 1
            ));
        }
    }

    #[test]
    fn joined_sources_keep_column_ambiguity_errors() {
        let schema = schema_with_join_tables();
        let error = analyze_sql_with_schema(
            &schema,
            "SELECT id FROM items JOIN categories ON items.id = categories.id",
        )
        .expect_err("unqualified duplicate join column is ambiguous");
        assert_eq!(error.to_string(), "Parse error: ambiguous column name: id");

        let document = analyze_sql_with_schema(
            &schema,
            "SELECT items.id FROM items \
             INNER JOIN categories ON items.id = categories.id",
        )
        .expect("explicit INNER JOIN binds");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let from = document.query(root.query).expect("query exists").blocks[0]
            .from
            .as_ref()
            .expect("joined query has FROM");
        assert_eq!(from.joins[0].kind, JoinKind::Inner);
    }

    #[test]
    fn using_joins_build_one_visible_column_from_resolved_sides() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT id, items.id, categories.id, * \
             FROM items JOIN categories USING (id)",
        )
        .expect("USING join binds into HIR");
        document.validate().expect("USING join produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let from = block.from.as_ref().expect("joined query has FROM");
        let JoinConstraint::Using(columns) = &from.joins[0].constraint else {
            panic!("USING remains explicit in HIR");
        };
        assert_eq!(columns.len(), 1);
        let column = &columns[0];
        assert_eq!(column.name, "id");
        assert_eq!(column.value, MergedColumnValue::Left);
        assert!(matches!(
            column.left.as_ref(),
            Expr::Column(reference) if reference.source == from.first && reference.column == 0
        ));
        assert_eq!(column.right.source, from.joins[0].right);
        assert_eq!(column.right.column, 0);

        assert!(matches!(block.outputs[0].expr, Expr::MergedColumn(_)));
        assert!(matches!(
            block.outputs[1].expr,
            Expr::Column(reference) if reference.source == from.first && reference.column == 0
        ));
        assert!(matches!(
            block.outputs[2].expr,
            Expr::Column(reference)
                if reference.source == from.joins[0].right && reference.column == 0
        ));
        assert_eq!(block.outputs.len(), 7);
        assert!(matches!(block.outputs[3].expr, Expr::MergedColumn(_)));
    }

    #[test]
    fn natural_joins_use_both_sides_for_comparison_and_left_for_visible_facts() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value, items.value, codes.value, * \
             FROM items NATURAL JOIN codes",
        )
        .expect("NATURAL join binds into HIR");
        document
            .validate()
            .expect("NATURAL join produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let from = block.from.as_ref().expect("joined query has FROM");
        let JoinConstraint::Natural(columns) = &from.joins[0].constraint else {
            panic!("NATURAL remains explicit in HIR");
        };
        assert_eq!(columns.len(), 1);
        let column = &columns[0];
        assert_eq!(column.name, "value");
        assert_eq!(column.type_fact.storage, Some(Type::Text));
        assert_eq!(
            column.affinity,
            crate::vdbe::affinity::Affinity::Text,
            "visible merged value keeps left affinity"
        );
        assert_eq!(
            column.comparison.components[0].affinity,
            crate::vdbe::affinity::Affinity::Numeric,
            "comparison sees the right INTEGER affinity"
        );
        assert_eq!(
            column
                .collation
                .as_ref()
                .expect("visible merged value keeps left collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert_eq!(
            column.comparison.components[0]
                .collation
                .as_ref()
                .expect("comparison resolves collation from both sides")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert!(matches!(block.outputs[0].expr, Expr::MergedColumn(_)));
        assert_eq!(block.outputs.len(), 7);
        assert!(matches!(block.outputs[3].expr, Expr::Column(_)));
        assert!(matches!(block.outputs[4].expr, Expr::MergedColumn(_)));
    }

    #[test]
    fn using_and_natural_joins_keep_name_errors() {
        let schema = schema_with_join_tables();
        let missing = analyze_sql_with_schema(
            &schema,
            "SELECT * FROM items JOIN categories USING (missing)",
        )
        .expect_err("USING requires the name on both sides");
        assert_eq!(
            missing.to_string(),
            "Parse error: cannot join using column missing - column not present in both tables"
        );

        let constrained = analyze_sql_with_schema(
            &schema,
            "SELECT * FROM items NATURAL JOIN categories ON items.id = categories.id",
        )
        .expect_err("NATURAL rejects an explicit constraint");
        assert_eq!(
            constrained.to_string(),
            "Parse error: a NATURAL join may not have an ON or USING clause"
        );

        let ambiguous = analyze_sql_with_schema(
            &schema,
            "SELECT * FROM items JOIN categories ON TRUE NATURAL JOIN extras",
        )
        .expect_err("NATURAL common name must be unique on the left");
        assert_eq!(
            ambiguous.to_string(),
            "Parse error: ambiguous column name: id"
        );
    }

    #[test]
    fn outer_using_joins_keep_kind_value_facts_and_star_order() {
        let schema = schema_with_join_tables();
        for (operator, kind, value) in [
            ("LEFT JOIN", JoinKind::Left, MergedColumnValue::Left),
            ("RIGHT JOIN", JoinKind::Right, MergedColumnValue::Right),
            ("FULL JOIN", JoinKind::Full, MergedColumnValue::Coalesce),
        ] {
            let sql = format!("SELECT * FROM items {operator} codes USING (value)");
            let document =
                analyze_sql_with_schema(&schema, &sql).expect("outer USING join binds into HIR");
            document
                .validate()
                .expect("outer USING join produces closed HIR");

            let HirRoot::Query(root) = &document.root else {
                panic!("SELECT produces query root");
            };
            let block = &document.query(root.query).expect("query exists").blocks[0];
            let from = block.from.as_ref().expect("joined query has FROM");
            let join = &from.joins[0];
            assert_eq!(join.kind, kind);
            let JoinConstraint::Using(columns) = &join.constraint else {
                panic!("outer USING remains explicit in HIR");
            };
            let column = &columns[0];
            assert_eq!(column.value, value);
            let Expr::MergedColumn(merged) = &block.outputs[1].expr else {
                panic!("star emits one merged USING column");
            };
            assert_eq!(merged.value, value);
            assert_eq!(
                block
                    .outputs
                    .iter()
                    .map(|output| output.name.as_str())
                    .collect::<Vec<_>>(),
                ["id", "value", "score", "label"],
                "RIGHT JOIN does not reorder lexical star columns"
            );

            match value {
                MergedColumnValue::Left => {
                    assert_eq!(column.type_fact.storage, Some(Type::Text));
                    assert_eq!(column.affinity, crate::vdbe::affinity::Affinity::Text);
                    assert!(column.has_affinity);
                    assert!(column.collation.is_some());
                }
                MergedColumnValue::Right => {
                    assert_eq!(column.type_fact.storage, Some(Type::Integer));
                    assert_eq!(column.affinity, crate::vdbe::affinity::Affinity::Integer);
                    assert!(column.has_affinity);
                    assert!(column.collation.is_none());
                }
                MergedColumnValue::Coalesce => {
                    assert!(column.type_fact.storage.is_none());
                    assert_eq!(column.affinity, crate::vdbe::affinity::Affinity::Blob);
                    assert!(!column.has_affinity);
                    assert!(column.collation.is_none());
                }
            }
        }
    }

    #[test]
    fn natural_outer_joins_use_the_same_merge_direction() {
        let schema = schema_with_join_tables();
        let document =
            analyze_sql_with_schema(&schema, "SELECT value FROM items NATURAL RIGHT JOIN codes")
                .expect("NATURAL RIGHT JOIN binds into HIR");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let join = &block.from.as_ref().expect("joined query has FROM").joins[0];
        assert_eq!(join.kind, JoinKind::Right);
        let JoinConstraint::Natural(columns) = &join.constraint else {
            panic!("NATURAL RIGHT JOIN remains explicit in HIR");
        };
        assert_eq!(columns[0].value, MergedColumnValue::Right);
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Integer));
    }

    #[test]
    fn right_join_after_another_join_keeps_existing_error() {
        let schema = schema_with_join_tables();
        let error = analyze_sql_with_schema(
            &schema,
            "SELECT * FROM items JOIN categories ON TRUE \
             RIGHT JOIN extras ON TRUE",
        )
        .expect_err("chained RIGHT JOIN remains unsupported");
        assert_eq!(
            error.to_string(),
            "Parse error: RIGHT JOIN following another join is not yet supported. \
             Try rewriting as LEFT JOIN or using a subquery."
        );
    }

    #[test]
    fn array_columns_keep_element_facts_without_program_metadata() {
        let mut schema = Schema::new();
        let table = Arc::new(
            BTreeTable::from_sql(
                "CREATE TABLE arrays(\
                    vals INTEGER[], matrix TEXT[][], anything ANY[]\
                 ) STRICT",
                2,
            )
            .expect("array table schema parses"),
        );
        schema
            .add_btree_table(table)
            .expect("array table name is unique");
        let document =
            analyze_sql_with_schema(&schema, "SELECT vals, matrix, anything FROM arrays")
                .expect("built-in array columns bind");
        document
            .validate()
            .expect("array type facts produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source_id = block.from.as_ref().expect("array query has source").first;
        let source = document.source(source_id).expect("array source exists");
        assert!(source.column_type_programs.iter().all(Option::is_none));

        let values = &source.columns[0].type_fact;
        assert_eq!(values.storage, Some(Type::Blob));
        assert_eq!(values.array_dimensions, 1);
        let value = values.array_element().expect("INTEGER[] has an element");
        assert_eq!(value.storage, Some(Type::Integer));
        assert_eq!(value.array_dimensions, 0);

        let matrix = &source.columns[1].type_fact;
        assert_eq!(matrix.storage, Some(Type::Blob));
        assert_eq!(matrix.array_dimensions, 2);
        let row = matrix.array_element().expect("TEXT[][] has array elements");
        assert_eq!(row.storage, Some(Type::Blob));
        assert_eq!(row.array_dimensions, 1);
        let value = row.array_element().expect("TEXT[] has scalar elements");
        assert_eq!(value.storage, Some(Type::Text));

        let anything = &source.columns[2].type_fact;
        assert!(anything
            .array_element()
            .expect("ANY[] has an element fact")
            .storage
            .is_none());
        assert!(block.outputs.iter().enumerate().all(|(column, output)| {
            matches!(
                output.expr,
                Expr::Column(reference)
                    if reference.source == source_id && reference.column == column
            )
        }));
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
    fn between_and_in_freeze_comparison_rules_in_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value BETWEEN 'a' AND 'z', \
             value NOT BETWEEN ('a' COLLATE RTRIM) AND 'z', \
             value IN (1, 'x'), 1 IN (value) FROM items",
        )
        .expect("BETWEEN and list IN expressions have valid SQL meaning");
        document
            .validate()
            .expect("BETWEEN and list IN produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 4);
        assert!(outputs.iter().all(|output| {
            output.type_fact.storage == Some(Type::Integer) && !output.has_affinity
        }));

        let Expr::Between {
            negated: false,
            start_comparison,
            end_comparison,
            ..
        } = &outputs[0].expr
        else {
            panic!("BETWEEN becomes HIR with two comparisons");
        };
        for comparison in [start_comparison, end_comparison] {
            assert_eq!(
                comparison.components[0].affinity,
                crate::vdbe::affinity::Affinity::Text
            );
            assert_eq!(
                comparison.components[0]
                    .collation
                    .as_ref()
                    .expect("column collation reaches both bounds")
                    .value(),
                &crate::translate::collate::CollationSeq::NoCase
            );
        }

        let Expr::Between {
            negated: true,
            start_comparison,
            end_comparison,
            ..
        } = &outputs[1].expr
        else {
            panic!("NOT BETWEEN preserves negation");
        };
        assert_eq!(
            start_comparison.components[0]
                .collation
                .as_ref()
                .expect("explicit bound collation wins")
                .value(),
            &crate::translate::collate::CollationSeq::Rtrim
        );
        assert_eq!(
            end_comparison.components[0]
                .collation
                .as_ref()
                .expect("other bound keeps column collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );

        let Expr::InList {
            negated: false,
            values,
            comparisons,
            ..
        } = &outputs[2].expr
        else {
            panic!("list IN becomes HIR");
        };
        assert_eq!(values.len(), 2);
        assert_eq!(comparisons.len(), 2);
        assert!(comparisons.iter().all(|comparison| {
            comparison.components[0].affinity == crate::vdbe::affinity::Affinity::Text
                && comparison.components[0]
                    .collation
                    .as_ref()
                    .is_some_and(|collation| {
                        collation.value() == &crate::translate::collate::CollationSeq::NoCase
                    })
        }));

        let Expr::InList { comparisons, .. } = &outputs[3].expr else {
            panic!("reversed list IN becomes HIR");
        };
        assert_eq!(comparisons.len(), 1);
        assert_eq!(
            comparisons[0].components[0].affinity,
            crate::vdbe::affinity::Affinity::Blob
        );
        assert_eq!(
            comparisons[0].components[0]
                .collation
                .as_ref()
                .expect("IN still uses collation from either operand")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
    }

    #[test]
    fn case_expressions_freeze_result_and_comparison_rules_in_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT CASE value \
                 WHEN 'a' THEN 1 \
                 WHEN ('b' COLLATE RTRIM) THEN 2.5 \
                 ELSE NULL END, \
             CASE WHEN id > 0 THEN value ELSE 'none' END, \
             CASE WHEN 0 THEN 1 END \
             FROM items",
        )
        .expect("simple and searched CASE expressions have valid SQL meaning");
        document
            .validate()
            .expect("CASE expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 3);
        assert!(outputs.iter().all(|output| !output.has_affinity));

        let Expr::Case {
            base: Some(_),
            when_then,
            else_expr: Some(_),
            base_comparisons,
        } = &outputs[0].expr
        else {
            panic!("simple CASE keeps base, branches, and ELSE");
        };
        assert_eq!(when_then.len(), 2);
        assert_eq!(base_comparisons.len(), 2);
        assert_eq!(outputs[0].type_fact.storage, Some(Type::Numeric));
        for comparison in base_comparisons {
            assert_eq!(
                comparison.components[0].affinity,
                crate::vdbe::affinity::Affinity::Text
            );
        }
        assert_eq!(
            base_comparisons[0].components[0]
                .collation
                .as_ref()
                .expect("base column supplies first comparison collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert_eq!(
            base_comparisons[1].components[0]
                .collation
                .as_ref()
                .expect("explicit WHEN collation wins")
                .value(),
            &crate::translate::collate::CollationSeq::Rtrim
        );

        let Expr::Case {
            base: None,
            when_then,
            else_expr: Some(_),
            base_comparisons,
        } = &outputs[1].expr
        else {
            panic!("searched CASE has branches and ELSE without base");
        };
        assert_eq!(when_then.len(), 1);
        assert!(base_comparisons.is_empty());
        assert_eq!(outputs[1].type_fact.storage, Some(Type::Text));

        let Expr::Case {
            base: None,
            when_then,
            else_expr: None,
            base_comparisons,
        } = &outputs[2].expr
        else {
            panic!("searched CASE can omit ELSE");
        };
        assert_eq!(when_then.len(), 1);
        assert!(base_comparisons.is_empty());
        assert_eq!(outputs[2].type_fact.storage, Some(Type::Integer));
    }

    #[test]
    fn builtin_casts_freeze_targets_and_parameters_in_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT CAST(value AS INTEGER), CAST(value AS TEXT), \
             CAST(value AS DECIMAL(10, 2)), CAST(value AS CHAR(255)), \
             CAST(value COLLATE RTRIM AS BLOB), CAST(value AS INTEGER[][]) \
             FROM items",
        )
        .expect("built-in CAST targets have valid SQL meaning");
        document
            .validate()
            .expect("built-in CAST expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 6);

        let expected = [
            (
                "INTEGER",
                Type::Integer,
                crate::vdbe::affinity::Affinity::Integer,
                0,
                0,
            ),
            (
                "TEXT",
                Type::Text,
                crate::vdbe::affinity::Affinity::Text,
                0,
                0,
            ),
            (
                "DECIMAL",
                Type::Numeric,
                crate::vdbe::affinity::Affinity::Numeric,
                2,
                0,
            ),
            (
                "CHAR",
                Type::Text,
                crate::vdbe::affinity::Affinity::Text,
                1,
                0,
            ),
            (
                "BLOB",
                Type::Blob,
                crate::vdbe::affinity::Affinity::Blob,
                0,
                0,
            ),
            (
                "INTEGER",
                Type::Blob,
                crate::vdbe::affinity::Affinity::Integer,
                0,
                2,
            ),
        ];
        for (output, (name, storage, affinity, parameters, dimensions)) in
            outputs.iter().zip(expected)
        {
            let Expr::Cast { target, .. } = &output.expr else {
                panic!("CAST becomes resolved HIR");
            };
            assert_eq!(target.name, name);
            assert_eq!(target.parameters.len(), parameters);
            assert_eq!(target.array_dimensions, dimensions);
            assert_eq!(target.type_fact.storage, Some(storage));
            assert_eq!(target.type_fact.array_dimensions, dimensions);
            assert_eq!(target.affinity, affinity);
            assert!(target.programs.encode.is_empty());
            assert!(target.programs.domain.is_none());
            assert!(target.programs.apply_builtin_affinity);
            assert_eq!(output.type_fact, target.type_fact);
            assert_eq!(output.affinity, affinity);
            assert!(output.has_affinity);
        }
        assert!(outputs[5].type_fact.is_array());
        assert_eq!(
            outputs[4]
                .collation
                .as_ref()
                .expect("CAST preserves operand collation")
                .value(),
            &crate::translate::collate::CollationSeq::Rtrim
        );
        assert!(outputs[4].collation_is_explicit);
    }

    #[test]
    fn custom_cast_without_programs_keeps_resolved_type_chain() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(&schema, "SELECT CAST(value AS BIGINT) FROM items")
            .expect("BIGINT needs no custom schema program");
        document
            .validate()
            .expect("catalog-resolved CAST produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("custom CAST becomes resolved HIR");
        };
        assert_eq!(target.name, "BIGINT");
        assert_eq!(target.type_fact.storage, Some(Type::Integer));
        assert_eq!(target.affinity, crate::vdbe::affinity::Affinity::Integer);
        assert_eq!(
            target
                .type_fact
                .declared
                .as_ref()
                .map(|value| value.name.as_str()),
            Some("BIGINT")
        );
        let chain = &target
            .type_fact
            .declared
            .as_ref()
            .expect("custom target keeps declaration")
            .custom_chain;
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].value().name, "bigint");
        assert_eq!(
            chain[0].database(),
            Some(DatabaseId::new(crate::MAIN_DB_ID))
        );
        assert_eq!(chain[0].snapshot(), document.snapshot);
        assert!(target.programs.encode.is_empty());
        assert!(target.programs.domain.is_none());
        assert!(!target.programs.apply_builtin_affinity);
        assert_eq!(output.type_fact, target.type_fact);
        assert_eq!(output.affinity, crate::vdbe::affinity::Affinity::Integer);
        assert!(output.has_affinity);
    }

    #[test]
    fn custom_cast_encoder_uses_a_document_owned_input_source() {
        let mut schema = schema_with_items();
        schema
            .add_type_from_sql(
                "CREATE TYPE positive(value INTEGER, minimum INTEGER) BASE INTEGER \
                 ENCODE CASE WHEN value > minimum THEN value ELSE NULL END",
            )
            .expect("custom type definition parses");
        let document =
            analyze_sql_with_schema(&schema, "SELECT CAST(value AS positive(0)) FROM items")
                .expect("simple custom encoder binds");
        document
            .validate()
            .expect("custom encoder produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("custom CAST becomes resolved HIR");
        };
        assert_eq!(target.programs.encode.len(), 1);
        assert!(!target.programs.apply_builtin_affinity);
        let call = &target.programs.encode[0];
        assert!(matches!(
            call.arguments.as_slice(),
            [Expr::Literal(ast::Literal::Numeric(value))] if value == "0"
        ));

        let program = document
            .schema_program(call.program)
            .expect("encoder program exists");
        let input = document
            .source(program.input_source)
            .expect("encoder input source exists");
        assert!(matches!(input.kind, SourceKind::SchemaExpression));
        assert_eq!(input.owner, SourceOwner::Root);
        assert_eq!(input.columns.len(), 2);
        assert_eq!(input.columns[0].name, "value");
        assert_eq!(input.columns[1].name, "minimum");
        assert_eq!(input.columns[0].type_fact.storage, Some(Type::Integer));

        let Expr::Case { when_then, .. } = &program.body else {
            panic!("stored CASE becomes HIR");
        };
        let Expr::Binary { lhs, rhs, .. } = &when_then[0].0 else {
            panic!("stored condition becomes binary HIR");
        };
        assert!(matches!(
            lhs.as_ref(),
            Expr::Column(column)
                if column.source == program.input_source && column.column == 0
        ));
        assert!(matches!(
            rhs.as_ref(),
            Expr::Column(column)
                if column.source == program.input_source && column.column == 1
        ));
        assert!(matches!(
            &when_then[0].1,
            Expr::Column(column)
                if column.source == program.input_source && column.column == 0
        ));
    }

    #[test]
    fn scalar_functions_keep_resolved_identity_and_result_type() {
        let schema = schema_with_items();
        let document =
            analyze_sql_with_schema(&schema, "SELECT length(value), abs(score) FROM items")
                .expect("scalar functions bind");
        document
            .validate()
            .expect("scalar functions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 2);
        for output in outputs {
            let Expr::Function(call) = &output.expr else {
                panic!("function call becomes resolved HIR");
            };
            assert!(matches!(call.evaluation, FunctionEvaluation::Scalar));
            assert!(matches!(call.operation, FunctionOperation::Ordinary));
            assert!(matches!(
                &call.arguments,
                crate::translate::semantic::hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } if values.len() == 1 && order_by.is_empty()
            ));
        }
        let Expr::Function(length) = &outputs[0].expr else {
            unreachable!();
        };
        assert!(matches!(
            length.function.value(),
            crate::function::Func::Scalar(crate::function::ScalarFunc::Length)
        ));
        assert_eq!(length.result_type.storage, Some(Type::Integer));
        assert_eq!(outputs[0].type_fact, length.result_type);
        let Expr::Function(abs) = &outputs[1].expr else {
            unreachable!();
        };
        assert!(matches!(
            abs.function.value(),
            crate::function::Func::Scalar(crate::function::ScalarFunc::Abs)
        ));
        assert_eq!(abs.result_type.storage, Some(Type::Real));
        assert_eq!(outputs[1].type_fact, abs.result_type);
    }

    #[test]
    fn custom_type_reads_freeze_types_and_member_indexes() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE telegram_msg AS STRUCT(chat_id INT, text TEXT)")
            .expect("struct type parses");
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram telegram_msg, slack TEXT)")
            .expect("union type parses");
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT union_tag(CAST(NULL AS platform)), \
                    struct_extract(\
                        union_extract(CAST(NULL AS platform), 'telegram'), \
                        'chat_id'\
                    )",
        )
        .expect("custom-type read functions bind");
        document
            .validate()
            .expect("custom-type reads produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;

        let Expr::Function(tag) = &outputs[0].expr else {
            panic!("union_tag becomes a function call");
        };
        let FunctionOperation::CustomType(CustomTypeOperation::UnionTag {
            union_type,
            tag_names,
        }) = &tag.operation
        else {
            panic!("union_tag keeps its resolved union operation");
        };
        assert_eq!(union_type.value().name, "platform");
        assert_eq!(tag_names.as_ref(), ["telegram", "slack"]);
        assert_eq!(tag.result_type.storage, Some(Type::Text));

        let Expr::Function(field) = &outputs[1].expr else {
            panic!("struct_extract becomes a function call");
        };
        let FunctionOperation::CustomType(CustomTypeOperation::StructExtract {
            struct_type,
            field_index,
        }) = &field.operation
        else {
            panic!("struct_extract keeps its resolved field operation");
        };
        assert_eq!(struct_type.value().name, "telegram_msg");
        assert_eq!(*field_index, 0);
        assert_eq!(field.result_type.storage, Some(Type::Integer));

        let [union, Expr::Literal(ast::Literal::String(field_name))] =
            field.arguments.expressions()
        else {
            panic!("struct_extract keeps its input and member name");
        };
        assert_eq!(field_name, "'chat_id'");
        let Expr::Function(union) = union else {
            panic!("struct input comes from union_extract");
        };
        let FunctionOperation::CustomType(CustomTypeOperation::UnionExtract {
            union_type,
            tag_index,
        }) = &union.operation
        else {
            panic!("union_extract keeps its resolved variant operation");
        };
        assert_eq!(union_type.value().name, "platform");
        assert_eq!(*tag_index, 0);
        assert_eq!(
            union
                .result_type
                .declared
                .as_ref()
                .map(|declaration| declaration.name.as_str()),
            Some("telegram_msg")
        );
    }

    #[test]
    fn custom_type_reads_keep_bind_time_errors() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram TEXT, slack TEXT)")
            .expect("union type parses");

        for (sql, expected) in [
            (
                "SELECT union_tag(1)",
                "Parse error: union_tag() argument must have a known union type",
            ),
            (
                "SELECT union_extract(CAST(NULL AS platform), 1)",
                "Parse error: union_extract() second argument must be a string literal",
            ),
            (
                "SELECT union_extract(CAST(NULL AS platform), \"telegram\")",
                "Parse error: union_extract() second argument must be a string literal",
            ),
            (
                "SELECT union_extract(CAST(NULL AS platform), 'discord')",
                "Parse error: unknown variant 'discord' in union type 'platform'",
            ),
            (
                "SELECT union_tag(CAST(NULL AS platform)) FILTER (WHERE TRUE)",
                "Parse error: union_tag() may not be used as an aggregate or window function",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid read must fail");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn sequence_writes_freeze_catalog_objects_and_names() {
        let schema = schema_with_sequence("orders");
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT nextval('orders'), \
                    setval('main.orders', 7), \
                    setval('orders', 8, TRUE), \
                    currval('orders')",
        )
        .expect("sequence functions bind");
        document
            .validate()
            .expect("sequence functions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        for (index, expected_kind, expected_user_name) in [
            (
                0,
                crate::translate::semantic::hir::SequenceOperationKind::NextValue,
                "orders",
            ),
            (
                1,
                crate::translate::semantic::hir::SequenceOperationKind::SetValue,
                "main.orders",
            ),
            (
                2,
                crate::translate::semantic::hir::SequenceOperationKind::SetValue,
                "orders",
            ),
        ] {
            let Expr::Function(call) = &outputs[index].expr else {
                panic!("sequence call becomes a function expression");
            };
            let FunctionOperation::Sequence(operation) = &call.operation else {
                panic!("sequence write keeps its resolved operation");
            };
            assert_eq!(operation.kind, expected_kind);
            assert_eq!(operation.user_name, expected_user_name);
            assert_eq!(operation.normalized_name, "orders");
            assert_eq!(operation.sequence.value().name, "orders");
            assert_eq!(
                operation.backing_table.value().get_name(),
                "__turso_internal_seq_orders"
            );
            assert_eq!(
                operation.sequence.database(),
                Some(DatabaseId::new(crate::MAIN_DB_ID))
            );
            assert_eq!(operation.sequence.snapshot(), document.snapshot);
            assert!(operation.sqlite_sequence.is_none());
            assert_eq!(call.result_type, TypeFact::known(Type::Integer));
        }

        let Expr::Function(currval) = &outputs[3].expr else {
            panic!("currval becomes a function expression");
        };
        assert!(matches!(currval.operation, FunctionOperation::Ordinary));
        assert_eq!(currval.result_type, TypeFact::known(Type::Integer));
    }

    #[test]
    fn sequence_writes_keep_name_errors() {
        let schema = schema_with_sequence("orders");
        for (sql, expected) in [
            (
                "SELECT nextval(orders)",
                "Parse error: expected a string literal argument",
            ),
            (
                "SELECT nextval(\"orders\")",
                "Parse error: expected a string literal argument",
            ),
            (
                "SELECT nextval('missing')",
                "Parse error: sequence \"missing\" does not exist",
            ),
            (
                "SELECT setval('aux.orders', 1)",
                "Invalid argument supplied: no such database: aux",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid call must fail");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn autoincrement_sequence_writes_resolve_sqlite_sequence() {
        let sequence_name = crate::schema::autoincrement_sequence_name("items");
        let mut schema = schema_with_sequence(&sequence_name);
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE sqlite_sequence(name, seq)", 3)
                    .expect("sqlite_sequence schema parses"),
            ))
            .expect("sqlite_sequence name is unique");
        let document =
            analyze_sql_with_schema(&schema, &format!("SELECT nextval('{sequence_name}')"))
                .expect("internal sequence binds");
        document
            .validate()
            .expect("internal sequence produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let Expr::Function(call) =
            &document.query(root.query).expect("query exists").blocks[0].outputs[0].expr
        else {
            panic!("nextval becomes a function expression");
        };
        let FunctionOperation::Sequence(operation) = &call.operation else {
            panic!("nextval keeps its resolved sequence operation");
        };
        let sqlite_sequence = operation
            .sqlite_sequence
            .as_ref()
            .expect("AUTOINCREMENT sequence resolves sqlite_sequence");
        assert_eq!(sqlite_sequence.value().get_name(), "sqlite_sequence");
        assert_eq!(sqlite_sequence.database(), operation.sequence.database());
    }

    #[test]
    fn aggregate_functions_get_stable_block_local_identities() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT sum(score), count(id), max(value), count(*), \
             sum(DISTINCT score), \
             group_concat(value ORDER BY score DESC NULLS LAST) FROM items",
        )
        .expect("plain aggregate calls bind");
        document
            .validate()
            .expect("aggregate calls produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 6);
        for (index, output) in block.outputs.iter().enumerate() {
            let Expr::Function(call) = &output.expr else {
                panic!("aggregate output becomes a function call");
            };
            assert!(matches!(
                &call.evaluation,
                FunctionEvaluation::Aggregate { id, filter: None }
                    if *id == crate::translate::semantic::hir::AggregateId::new(block.id, index)
            ));
        }
        assert!(block.outputs[..3].iter().all(|output| {
            matches!(
                &output.expr,
                Expr::Function(call)
                    if matches!(
                        &call.arguments,
                        crate::translate::semantic::hir::FunctionArguments::Expressions {
                            values,
                            distinctness: None,
                            order_by,
                        } if values.len() == 1 && order_by.is_empty()
                    )
            )
        }));
        let Expr::Function(count_star) = &block.outputs[3].expr else {
            unreachable!();
        };
        assert!(matches!(
            &count_star.arguments,
            crate::translate::semantic::hir::FunctionArguments::Star
        ));
        let Expr::Function(distinct_sum) = &block.outputs[4].expr else {
            unreachable!();
        };
        assert!(matches!(
            &distinct_sum.arguments,
            crate::translate::semantic::hir::FunctionArguments::Expressions {
                values,
                distinctness: Some(ast::Distinctness::Distinct),
                order_by,
            } if values.len() == 1 && order_by.is_empty()
        ));
        let Expr::Function(ordered_concat) = &block.outputs[5].expr else {
            unreachable!();
        };
        let crate::translate::semantic::hir::FunctionArguments::Expressions {
            values,
            distinctness,
            order_by,
        } = &ordered_concat.arguments
        else {
            panic!("ordered aggregate has expression arguments");
        };
        assert_eq!(values.len(), 1);
        assert!(distinctness.is_none());
        assert_eq!(order_by.len(), 1);
        assert!(matches!(
            order_by[0].expr,
            Expr::Column(crate::translate::semantic::hir::ColumnRef { column: 2, .. })
        ));
        assert_eq!(order_by[0].order, ast::SortOrder::Desc);
        assert_eq!(order_by[0].nulls, Some(ast::NullsOrder::Last));
        assert_eq!(order_by[0].type_fact.storage, Some(Type::Real));
        assert!(order_by[0].collation.is_none());
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Numeric));
        assert_eq!(block.outputs[1].type_fact.storage, Some(Type::Integer));
        assert_eq!(block.outputs[2].type_fact.storage, Some(Type::Text));
        assert_eq!(block.outputs[3].type_fact.storage, Some(Type::Integer));
        assert_eq!(block.outputs[4].type_fact.storage, Some(Type::Numeric));
        assert_eq!(block.outputs[5].type_fact.storage, Some(Type::Text));
    }

    #[test]
    fn aggregate_filters_are_bound_into_evaluation() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT sum(score) FILTER (WHERE id > 0), \
             count(*) FILTER (WHERE value IS NOT NULL) FROM items",
        )
        .expect("aggregate FILTER clauses bind");
        document
            .validate()
            .expect("aggregate filters produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 2);

        let Expr::Function(sum) = &block.outputs[0].expr else {
            panic!("filtered aggregate becomes a function call");
        };
        assert!(matches!(
            &sum.evaluation,
            FunctionEvaluation::Aggregate {
                id,
                filter: Some(filter),
            } if *id == crate::translate::semantic::hir::AggregateId::new(block.id, 0)
                && matches!(filter.as_ref(), Expr::Binary { .. })
        ));

        let Expr::Function(count) = &block.outputs[1].expr else {
            panic!("filtered COUNT becomes a function call");
        };
        assert!(matches!(
            count.arguments,
            crate::translate::semantic::hir::FunctionArguments::Star
        ));
        assert!(matches!(
            &count.evaluation,
            FunctionEvaluation::Aggregate {
                id,
                filter: Some(filter),
            } if *id == crate::translate::semantic::hir::AggregateId::new(block.id, 1)
                && matches!(filter.as_ref(), Expr::NotNull(_))
        ));
    }

    #[test]
    fn inline_windows_keep_resolved_specs_and_block_local_identities() {
        use crate::translate::semantic::hir::WindowFrameBound;

        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT row_number() OVER ( \
                 PARTITION BY value ORDER BY score DESC NULLS LAST \
                 ROWS BETWEEN missing PRECEDING AND missing FOLLOWING), \
             sum(score) FILTER (WHERE id > 0) OVER ( \
                 PARTITION BY value ORDER BY id \
                 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), \
             first_value(value) OVER (ORDER BY score) \
             FROM items",
        )
        .expect("inline windows bind");
        document
            .validate()
            .expect("inline windows produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 0);
        assert_eq!(block.window_function_count, 3);

        let Expr::Function(row_number) = &block.outputs[0].expr else {
            panic!("row_number becomes a function call");
        };
        let FunctionEvaluation::Window {
            id,
            window,
            filter: None,
        } = &row_number.evaluation
        else {
            panic!("row_number has window evaluation");
        };
        assert_eq!(
            *id,
            crate::translate::semantic::hir::WindowFunctionId::new(block.id, 0)
        );
        let window = &block.windows[window.index];
        assert_eq!(window.partition_by.len(), 1);
        assert_eq!(window.order_by.len(), 1);
        assert_eq!(window.order_by[0].order, ast::SortOrder::Desc);
        assert_eq!(window.order_by[0].nulls, Some(ast::NullsOrder::Last));
        let frame = &window.frame;
        assert_eq!(frame.mode, ast::FrameMode::Rows);
        assert!(matches!(frame.start, WindowFrameBound::UnboundedPreceding));
        assert!(matches!(frame.end, Some(WindowFrameBound::CurrentRow)));
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Integer));

        let Expr::Function(sum) = &block.outputs[1].expr else {
            panic!("window sum becomes a function call");
        };
        let FunctionEvaluation::Window {
            id,
            window,
            filter: Some(filter),
        } = &sum.evaluation
        else {
            panic!("sum has window evaluation and filter");
        };
        assert_eq!(
            *id,
            crate::translate::semantic::hir::WindowFunctionId::new(block.id, 1)
        );
        assert!(matches!(filter.as_ref(), Expr::Binary { .. }));
        let frame = &block.windows[window.index].frame;
        assert_eq!(frame.mode, ast::FrameMode::Rows);
        assert!(matches!(frame.start, WindowFrameBound::Preceding(_)));
        assert!(matches!(frame.end, Some(WindowFrameBound::Following(_))));
        assert_eq!(block.outputs[1].type_fact.storage, Some(Type::Numeric));

        let Expr::Function(first_value) = &block.outputs[2].expr else {
            panic!("first_value becomes a function call");
        };
        let FunctionEvaluation::Window { window, .. } = &first_value.evaluation else {
            panic!("first_value has window evaluation");
        };
        let frame = &block.windows[window.index].frame;
        assert_eq!(frame.mode, ast::FrameMode::Range);
        assert!(matches!(frame.start, WindowFrameBound::UnboundedPreceding));
        assert!(matches!(frame.end, Some(WindowFrameBound::CurrentRow)));
        assert_eq!(block.outputs[2].type_fact.storage, Some(Type::Text));
    }

    #[test]
    fn inline_windows_keep_existing_restrictions() {
        let schema = schema_with_items();
        for (sql, expected) in [
            (
                "SELECT row_number() FROM items",
                "Parse error: misuse of window function: row_number()",
            ),
            (
                "SELECT length(value) OVER () FROM items",
                "Parse error: length may not be used as a window function",
            ),
            (
                "SELECT row_number() FILTER (WHERE id > 0) OVER () FROM items",
                "Parse error: FILTER clause may only be used with aggregate window functions",
            ),
            (
                "SELECT sum(DISTINCT score) OVER () FROM items",
                "Parse error: DISTINCT is not supported for window functions",
            ),
            (
                "SELECT sum(score ORDER BY id) OVER () FROM items",
                "Parse error: ORDER BY clause is not supported yet in aggregate functions",
            ),
            (
                "SELECT sum(score) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM items",
                "Parse error: RANGE with offset PRECEDING/FOLLOWING requires one ORDER BY expression",
            ),
            (
                "SELECT array_agg(value) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM items",
                "Parse error: array_agg() does not yet support window frames with a moving start; use a frame with UNBOUNDED PRECEDING start",
            ),
            (
                "SELECT sum(max(score)) OVER () FROM items",
                "Parse error: misuse of aggregate function max()",
            ),
            (
                "SELECT sum(row_number() OVER ()) FROM items",
                "Parse error: misuse of window function: row_number()",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("unsupported inline window form is rejected");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn named_windows_resolve_inheritance_into_block_owned_windows() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT rank() OVER ranked, sum(score) OVER base, \
                    row_number() OVER (later ORDER BY score) \
             FROM items \
             WINDOW base AS (PARTITION BY value), \
                    ranked AS (base ORDER BY score), \
                    later AS (PARTITION BY value)",
        )
        .expect("named and inline inheritance bind");
        document
            .validate()
            .expect("resolved windows produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.window_function_count, 3);
        assert_eq!(block.windows.len(), 3);
        for (index, output) in block.outputs.iter().enumerate() {
            let Expr::Function(function) = &output.expr else {
                panic!("window output becomes a function call");
            };
            let FunctionEvaluation::Window { window, .. } = function.evaluation else {
                panic!("function has window evaluation");
            };
            assert_eq!(window.index, index);
            assert_eq!(block.windows[index].id, window);
        }
        assert_eq!(block.windows[0].partition_by.len(), 1);
        assert_eq!(block.windows[0].order_by.len(), 1);
        assert_eq!(block.windows[1].partition_by.len(), 1);
        assert!(block.windows[1].order_by.is_empty());
        assert_eq!(block.windows[2].partition_by.len(), 1);
        assert_eq!(block.windows[2].order_by.len(), 1);
    }

    #[test]
    fn named_windows_keep_sqlite_chaining_rules() {
        let schema = schema_with_items();

        analyze_sql_with_schema(
            &schema,
            "SELECT row_number() OVER framed FROM items \
             WINDOW framed AS (ORDER BY score ROWS BETWEEN missing PRECEDING AND CURRENT ROW)",
        )
        .expect("coerced window functions ignore named user frames");

        for (sql, expected) in [
            (
                "SELECT sum(score) OVER absent FROM items",
                "Parse error: no such window: absent",
            ),
            (
                "SELECT sum(score) OVER (base PARTITION BY id) FROM items \
                 WINDOW base AS (PARTITION BY value)",
                "Parse error: cannot override PARTITION clause of window: base",
            ),
            (
                "SELECT sum(score) OVER (base ORDER BY id) FROM items \
                 WINDOW base AS (ORDER BY score)",
                "Parse error: cannot override ORDER BY clause of window: base",
            ),
            (
                "SELECT sum(score) OVER child FROM items \
                 WINDOW base AS (ROWS CURRENT ROW), child AS (base)",
                "Parse error: cannot override frame specification of window: base",
            ),
            (
                "SELECT sum(score) OVER second FROM items \
                 WINDOW first AS (), second AS (later), later AS ()",
                "Parse error: no such window: later",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid chain fails");
            assert_eq!(error.to_string(), expected, "SQL: {sql}");
        }
    }

    #[test]
    fn ordered_set_aggregates_keep_direct_and_ordered_inputs_separate() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT mode() WITHIN GROUP (ORDER BY value), \
             percentile_cont(0.5) WITHIN GROUP (ORDER BY score), \
             percentile_disc(0.5) WITHIN GROUP (ORDER BY value COLLATE BINARY) \
                 FILTER (WHERE id > 0) FROM items",
        )
        .expect("ordered-set aggregates bind");
        document
            .validate()
            .expect("ordered-set aggregates produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 3);

        let Expr::Function(mode) = &block.outputs[0].expr else {
            panic!("mode becomes a function call");
        };
        let crate::translate::semantic::hir::FunctionArguments::OrderedSet { direct, order_by } =
            &mode.arguments
        else {
            panic!("mode keeps ordered-set arguments");
        };
        assert!(direct.is_empty());
        assert!(matches!(order_by.expr, Expr::Column(_)));
        assert_eq!(order_by.type_fact.storage, Some(Type::Text));
        assert!(order_by.collation.is_some());
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Text));

        let Expr::Function(percentile_cont) = &block.outputs[1].expr else {
            panic!("percentile_cont becomes a function call");
        };
        let crate::translate::semantic::hir::FunctionArguments::OrderedSet { direct, order_by } =
            &percentile_cont.arguments
        else {
            panic!("percentile_cont keeps ordered-set arguments");
        };
        assert_eq!(direct.len(), 1);
        assert!(matches!(order_by.expr, Expr::Column(_)));
        assert_eq!(order_by.type_fact.storage, Some(Type::Real));
        assert_eq!(block.outputs[1].type_fact.storage, Some(Type::Real));

        let Expr::Function(percentile_disc) = &block.outputs[2].expr else {
            panic!("percentile_disc becomes a function call");
        };
        let crate::translate::semantic::hir::FunctionArguments::OrderedSet { direct, order_by } =
            &percentile_disc.arguments
        else {
            panic!("percentile_disc keeps ordered-set arguments");
        };
        assert_eq!(direct.len(), 1);
        assert!(matches!(order_by.expr, Expr::Collate { .. }));
        assert_eq!(order_by.order, ast::SortOrder::Asc);
        assert!(order_by.nulls.is_none());
        assert!(order_by.collation.is_some());
        assert_eq!(block.outputs[2].type_fact.storage, Some(Type::Text));
        assert!(matches!(
            &percentile_disc.evaluation,
            FunctionEvaluation::Aggregate {
                filter: Some(filter),
                ..
            } if matches!(filter.as_ref(), Expr::Binary { .. })
        ));
    }

    #[test]
    fn ordered_set_aggregates_keep_existing_restrictions() {
        let schema = schema_with_items();
        for (sql, expected) in [
            (
                "SELECT sum(score) WITHIN GROUP (ORDER BY score) FROM items",
                "Parse error: WITHIN GROUP is not supported for function sum()",
            ),
            (
                "SELECT mode(score) WITHIN GROUP (ORDER BY value) FROM items",
                "Parse error: wrong number of arguments to function mode()",
            ),
            (
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY score DESC) FROM items",
                "Parse error: DESC and NULLS ordering inside WITHIN GROUP are not supported yet",
            ),
            (
                "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY score, id) FROM items",
                "Parse error: WITHIN GROUP for percentile_disc() must specify exactly one ORDER BY expression",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("unsupported ordered-set form is rejected");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn aggregate_checkpoint_rejects_nested_and_modified_calls() {
        let schema = schema_with_items();
        let nested = analyze_sql_with_schema(&schema, "SELECT sum(max(score)) FROM items")
            .expect_err("aggregate calls cannot be nested");
        assert_eq!(
            nested.to_string(),
            "Parse error: misuse of aggregate function max()"
        );
        let nested_filter = analyze_sql_with_schema(
            &schema,
            "SELECT sum(score) FILTER (WHERE max(id) > 0) FROM items",
        )
        .expect_err("aggregate filters cannot contain aggregates");
        assert_eq!(
            nested_filter.to_string(),
            "Parse error: misuse of aggregate function max()"
        );

        for sql in [
            "SELECT length(DISTINCT value) FROM items",
            "SELECT length(value ORDER BY score) FROM items",
            "SELECT length(value) FILTER (WHERE id > 0) FROM items",
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("scalar functions reject aggregate modifiers");
            assert_eq!(
                error.to_string(),
                "Parse error: semantic analysis accepts source-free literal SELECT statements"
            );
        }
    }

    #[test]
    fn custom_cast_encoder_can_call_scalar_functions() {
        let mut schema = schema_with_items();
        schema
            .add_type_from_sql(
                "CREATE TYPE short_text(value TEXT, maximum INTEGER) BASE TEXT \
                 ENCODE CASE WHEN length(value) <= maximum THEN value ELSE NULL END",
            )
            .expect("custom type definition parses");
        let document =
            analyze_sql_with_schema(&schema, "SELECT CAST(value AS short_text(3)) FROM items")
                .expect("function call in custom encoder binds");
        document
            .validate()
            .expect("function encoder produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("custom CAST becomes resolved HIR");
        };
        let program = document
            .schema_program(target.programs.encode[0].program)
            .expect("encoder program exists");
        let Expr::Case { when_then, .. } = &program.body else {
            panic!("encoder keeps CASE body");
        };
        let Expr::Binary { lhs, .. } = &when_then[0].0 else {
            panic!("encoder condition is a comparison");
        };
        let Expr::Function(length) = lhs.as_ref() else {
            panic!("comparison contains resolved function");
        };
        assert!(matches!(
            length.function.value(),
            crate::function::Func::Scalar(crate::function::ScalarFunc::Length)
        ));
        assert!(matches!(length.evaluation, FunctionEvaluation::Scalar));
        assert_eq!(length.result_type.storage, Some(Type::Integer));
        assert!(matches!(
            length.arguments.expressions(),
            [Expr::Column(column)]
                if column.source == program.input_source && column.column == 0
        ));
    }

    #[test]
    fn varchar_encoder_keeps_abort_raise_in_hir() {
        let schema = schema_with_items();
        let document =
            analyze_sql_with_schema(&schema, "SELECT CAST(value AS VARCHAR(3)) FROM items")
                .expect("built-in VARCHAR encoder binds");
        document
            .validate()
            .expect("VARCHAR encoder produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("VARCHAR CAST becomes resolved HIR");
        };
        assert_eq!(target.programs.encode.len(), 1);
        let program = document
            .schema_program(target.programs.encode[0].program)
            .expect("VARCHAR encoder program exists");
        let Expr::Case {
            else_expr: Some(else_expr),
            ..
        } = &program.body
        else {
            panic!("VARCHAR encoder keeps failure branch");
        };
        let Expr::Raise {
            action: ast::ResolveType::Abort,
            message: Some(message),
        } = else_expr.as_ref()
        else {
            panic!("VARCHAR failure branch becomes RAISE(ABORT)");
        };
        assert!(matches!(
            message.as_ref(),
            Expr::Literal(ast::Literal::String(value))
                if value == "'value too long for varchar'"
        ));
    }

    #[test]
    fn non_trigger_raise_only_allows_abort() {
        let document = analyze_sql("SELECT RAISE(ABORT, 'stop')")
            .expect("Turso allows RAISE(ABORT) outside triggers");
        document
            .validate()
            .expect("standalone abort produces closed HIR");

        let error = analyze_sql("SELECT RAISE(FAIL, 'stop')")
            .expect_err("other RAISE actions require a trigger");
        assert_eq!(
            error.to_string(),
            "Parse error: RAISE() may only be used within a trigger-program"
        );
    }

    #[test]
    fn domain_cast_keeps_inherited_not_null_and_check_programs() {
        let mut schema = schema_with_items();
        schema
            .add_type_from_sql(
                "CREATE DOMAIN positive_integer AS INTEGER \
                 CONSTRAINT positive CHECK (value > 0)",
            )
            .expect("parent domain definition parses");
        schema
            .add_type_from_sql("CREATE DOMAIN required_positive AS positive_integer NOT NULL")
            .expect("child domain definition parses");
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT CAST(value AS required_positive) FROM items",
        )
        .expect("domain constraints bind");
        document
            .validate()
            .expect("domain constraints produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("domain CAST becomes resolved HIR");
        };
        let domain = target
            .programs
            .domain
            .as_ref()
            .expect("domain CAST carries constraints");
        assert_eq!(
            domain.not_null_description.as_deref(),
            Some("domain required_positive does not allow null values")
        );
        assert_eq!(domain.checks.len(), 1);
        assert_eq!(
            domain.checks[0].failure_description,
            "value for domain positive_integer violates check constraint \"positive\""
        );
        assert!(domain.checks[0].call.arguments.is_empty());

        let program = document
            .schema_program(domain.checks[0].call.program)
            .expect("domain CHECK program exists");
        let input = document
            .source(program.input_source)
            .expect("domain CHECK input exists");
        assert_eq!(input.columns.len(), 1);
        assert_eq!(input.columns[0].name, "value");
        assert_eq!(input.columns[0].type_fact.storage, target.type_fact.storage);
        assert!(input.columns[0].type_fact.declared.is_none());
        let Expr::Binary { lhs, .. } = &program.body else {
            panic!("domain CHECK becomes binary HIR");
        };
        assert!(matches!(
            lhs.as_ref(),
            Expr::Column(column)
                if column.source == program.input_source && column.column == 0
        ));
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
