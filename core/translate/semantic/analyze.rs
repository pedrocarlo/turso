use turso_parser::ast;

use crate::{
    numeric::Numeric, schema::Type, util::parse_numeric_literal, vdbe::affinity::Affinity,
    LimboError, Result, Value, MAIN_DB_ID,
};

use super::{
    context::SemanticContext,
    hir::{
        BoundSchemaProgram, Cte, DatabaseId, DatabaseSnapshot, Expr, HirDocument, HirRoot, Output,
        OutputId, OutputNameKind, Query, QueryBlock, QueryBlockBody, QueryBlockId, QueryId,
        QueryRoot, Source, TypeFact,
    },
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

struct Analyzer<'context, 'catalog> {
    context: &'context SemanticContext<'catalog>,
    queries: Vec<Option<Query>>,
    sources: Vec<Option<Source>>,
    ctes: Vec<Option<Cte>>,
    schema_programs: Vec<Option<BoundSchemaProgram>>,
}

impl<'context, 'catalog> Analyzer<'context, 'catalog> {
    fn new(context: &'context SemanticContext<'catalog>) -> Self {
        Self {
            context,
            queries: Vec::new(),
            sources: Vec::new(),
            ctes: Vec::new(),
            schema_programs: Vec::new(),
        }
    }

    fn reserve_query(&mut self) -> QueryId {
        let id = QueryId::new(self.queries.len());
        self.queries.push(None);
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
        if from.is_some()
            || where_clause.is_some()
            || group_by.is_some()
            || !window_clause.is_empty()
        {
            return unsupported_select();
        }

        let query_id = self.reserve_query();
        let block_id = QueryBlockId::new(query_id, 0);
        let outputs = columns
            .iter()
            .enumerate()
            .map(|(index, column)| analyze_literal_output(block_id, index, column))
            .collect::<Result<Vec<_>>>()?;
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
                    from: None,
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
}

fn analyze_literal_output(
    block: QueryBlockId,
    index: usize,
    column: &ast::ResultColumn,
) -> Result<Output> {
    let ast::ResultColumn::Expr(expression, alias) = column else {
        return unsupported_select();
    };
    let ast::Expr::Literal(literal) = expression.as_ref() else {
        return unsupported_select();
    };
    let (name, name_kind) = match alias {
        Some(alias) if alias.is_explicit() => (
            alias.name().as_str().to_string(),
            OutputNameKind::ExplicitAlias,
        ),
        Some(ast::As::ImplicitColumnName(name)) => {
            (name.as_str().to_string(), OutputNameKind::Inferred)
        }
        None => (expression.to_string(), OutputNameKind::Inferred),
        Some(_) => unreachable!("all explicit aliases were handled"),
    };
    let type_fact = literal_type_fact(literal)?;

    Ok(Output {
        id: OutputId::query(block, index),
        name,
        expr: Expr::Literal(literal.clone()),
        type_fact,
        affinity: Affinity::Blob,
        schema_affinity: Affinity::Blob,
        has_affinity: false,
        collation: None,
        collation_is_explicit: false,
        name_kind,
    })
}

fn literal_type_fact(literal: &ast::Literal) -> Result<TypeFact> {
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

fn unsupported_select<T>() -> Result<T> {
    Err(LimboError::ParseError(
        "semantic analysis accepts source-free literal SELECT statements".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use turso_parser::{ast, parser::Parser};

    use crate::{
        dialect::SqliteDialect,
        schema::{Schema, Type},
        sync::Arc,
        SymbolTable,
    };

    use super::*;
    use crate::translate::semantic::hir::{HirRoot, OutputNameKind};

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
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            &schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        let statement = parse_statement(sql);
        analyze(&context, AnalyzeInput::Statement(&statement))
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
        assert!(error
            .to_string()
            .contains("source-free literal SELECT statements"));
    }
}
