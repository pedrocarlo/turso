//! Runtime destination preparation for resolved HIR queries.

use turso_parser::ast::SortOrder;

use crate::{
    schema::{Index, IndexColumn},
    sync::Arc,
    translate::{
        eqp::EqpDetail,
        plan::QueryDestination,
        semantic::hir::{HirDocument, QueryId, SubqueryExpr},
    },
    vdbe::{
        builder::{CursorType, ProgramBuilder, SubqueryBinding},
        insn::Insn,
    },
    LimboError, Result,
};

/// Physical destination and execution facts for one expression subquery.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct PreparedSubquery {
    pub(crate) query: QueryId,
    pub(crate) destination: QueryDestination,
    pub(crate) correlated: bool,
}

struct DestinationBinding {
    destination: QueryDestination,
    binding: SubqueryBinding,
}

/// Allocate the legacy query destination from facts already frozen in HIR.
///
/// The matching expression binding is installed at the same time so query
/// emission and expression lowering cannot choose different storage.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn prepare_subquery(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &SubqueryExpr,
) -> Result<PreparedSubquery> {
    let query_id = match expression {
        SubqueryExpr::Scalar { query, .. }
        | SubqueryExpr::Row { query }
        | SubqueryExpr::In { query, .. }
        | SubqueryExpr::Exists(query) => *query,
    };
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR subquery destination references missing query {query_id}"
        ))
    })?;
    let width = query.output.len();
    if width == 0 {
        return Err(LimboError::InternalError(format!(
            "HIR subquery {query_id} has no outputs"
        )));
    }

    let prepared = match expression {
        SubqueryExpr::Scalar { output, .. } => {
            if *output >= width {
                return Err(LimboError::InternalError(format!(
                    "HIR scalar subquery {query_id} output {output} exceeds query width {width}"
                )));
            }
            row_value_destination(program, width)
        }
        SubqueryExpr::Row { .. } => row_value_destination(program, width),
        SubqueryExpr::Exists(_) => {
            let register = program.alloc_register();
            DestinationBinding {
                destination: QueryDestination::ExistsSubqueryResult {
                    result_reg: register,
                },
                binding: SubqueryBinding::Exists { register },
            }
        }
        SubqueryExpr::In { comparison, .. } => {
            if comparison.components.len() != width {
                return Err(LimboError::InternalError(format!(
                    "HIR IN subquery {query_id} comparison width {} does not match query width {width}",
                    comparison.components.len()
                )));
            }
            let columns = query
                .output
                .iter()
                .zip(&comparison.components)
                .enumerate()
                .map(|(position, (output, component))| {
                    let output = document.output(*output).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR IN subquery {query_id} references missing output {output:?}"
                        ))
                    })?;
                    Ok(IndexColumn {
                        name: output.name.clone(),
                        order: SortOrder::Asc,
                        nulls_order: None,
                        pos_in_table: position,
                        collation: component
                            .collation
                            .as_ref()
                            .map(|collation| *collation.value()),
                        default: None,
                        expr: None,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let affinity_str = comparison
                .components
                .iter()
                .map(|component| component.affinity.aff_mask())
                .collect::<String>();
            let index = Arc::new(Index {
                columns,
                name: format!("ephemeral_index_hir_sub_{}", query_id.index()),
                table_name: String::new(),
                root_page: 0,
                unique: false,
                ephemeral: true,
                has_rowid: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            });
            let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            DestinationBinding {
                destination: QueryDestination::EphemeralIndex {
                    cursor_id: cursor,
                    index,
                    affinity_str: Some(Arc::new(affinity_str)),
                    is_delete: false,
                },
                binding: SubqueryBinding::InIndex { cursor },
            }
        }
    };

    program.bind_subquery(query_id, prepared.binding);
    Ok(PreparedSubquery {
        query: query_id,
        destination: prepared.destination,
        correlated: !query.captures.is_empty(),
    })
}

/// Emit the execution shell shared by all non-FROM expression subqueries.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn emit_prepared_subquery(
    program: &mut ProgramBuilder,
    prepared: &PreparedSubquery,
    emit_body: impl FnOnce(&mut ProgramBuilder, QueryId, &QueryDestination) -> Result<()>,
) -> Result<()> {
    program.nested(|program| {
        let explain_id = program.next_subquery_eqp_id();
        match &prepared.destination {
            QueryDestination::ExistsSubqueryResult { .. } => {}
            QueryDestination::RowValueSubqueryResult { .. } => {
                crate::emit_explain!(
                    program,
                    true,
                    EqpDetail::ScalarSubquery {
                        id: explain_id,
                        correlated: prepared.correlated,
                    }
                );
            }
            QueryDestination::EphemeralIndex { .. } => {
                crate::emit_explain!(
                    program,
                    true,
                    EqpDetail::ListSubquery {
                        id: explain_id,
                        correlated: prepared.correlated,
                    }
                );
            }
            destination => {
                return Err(LimboError::InternalError(format!(
                    "HIR expression subquery has invalid destination {destination:?}"
                )));
            }
        }

        let once_done = if prepared.correlated {
            None
        } else {
            let done = program.allocate_label();
            program.emit_insn(Insn::Once {
                target_pc_when_reentered: done,
            });
            Some(done)
        };

        match &prepared.destination {
            QueryDestination::ExistsSubqueryResult { result_reg } => {
                let return_register = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: return_register,
                    dest_end: None,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
                emit_body(program, prepared.query, &prepared.destination)?;
                program.emit_insn(Insn::Return {
                    return_reg: return_register,
                    can_fallthrough: true,
                });
            }
            QueryDestination::RowValueSubqueryResult {
                result_reg_start,
                num_regs,
            } => {
                let return_register = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: return_register,
                    dest_end: None,
                });
                for register in *result_reg_start..*result_reg_start + *num_regs {
                    program.emit_insn(Insn::Null {
                        dest: register,
                        dest_end: None,
                    });
                }
                emit_body(program, prepared.query, &prepared.destination)?;
                program.emit_insn(Insn::Return {
                    return_reg: return_register,
                    can_fallthrough: true,
                });
            }
            QueryDestination::EphemeralIndex { cursor_id, .. } => {
                program.emit_insn(Insn::OpenEphemeral {
                    cursor_id: *cursor_id,
                    is_table: false,
                });
                emit_body(program, prepared.query, &prepared.destination)?;
            }
            _ => unreachable!("expression subquery destination was checked"),
        }

        if !matches!(
            prepared.destination,
            QueryDestination::ExistsSubqueryResult { .. }
        ) {
            program.pop_current_parent_explain();
        }
        if let Some(once_done) = once_done {
            program.preassign_label_to_next_insn(once_done);
        }
        Ok(())
    })
}

fn row_value_destination(program: &mut ProgramBuilder, width: usize) -> DestinationBinding {
    let start = program.alloc_registers(width);
    DestinationBinding {
        destination: QueryDestination::RowValueSubqueryResult {
            result_reg_start: start,
            num_regs: width,
        },
        binding: SubqueryBinding::RowValue {
            start,
            count: width,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dialect::SqliteDialect,
        schema::{BTreeTable, Schema},
        translate::semantic::{
            catalog::{SemanticCatalog, SemanticCatalogDatabase},
            context::DoubleQuotedDml,
            hir::{self, Expr, HirRoot},
            SemanticOptions, SemanticRootInput,
        },
        vdbe::builder::{ProgramBuilderOpts, QueryMode},
        SymbolTable, MAIN_DB_ID,
    };
    use turso_parser::parser::Parser;

    fn program() -> ProgramBuilder {
        program_with_mode(QueryMode::Normal)
    }

    fn program_with_mode(mode: QueryMode) -> ProgramBuilder {
        ProgramBuilder::new(mode, None, ProgramBuilderOpts::new(0, 4, 0))
    }

    fn analyze_sql(mut schema: Schema, sql: &str) -> HirDocument {
        schema
            .resolve_all_custom_type_affinities()
            .expect("custom affinities resolve");
        let catalog = SemanticCatalog {
            databases: vec![SemanticCatalogDatabase {
                id: hir::DatabaseId::new(MAIN_DB_ID),
                name: "main".to_string(),
                schema: Arc::new(schema),
            }],
            unqualified_database_search_path: vec![hir::DatabaseId::new(MAIN_DB_ID)],
        };
        let command = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains a statement");
        let turso_parser::ast::Cmd::Stmt(statement) = command else {
            panic!("SQL contains a statement command");
        };
        crate::translate::semantic::analyze_root(
            &catalog,
            &SymbolTable::new(),
            SemanticOptions {
                dialect: Arc::new(SqliteDialect),
                custom_types_enabled: true,
                dqs_dml: DoubleQuotedDml::Enabled,
            },
            SemanticRootInput::Statement(&statement),
        )
        .map(|document| {
            document.validate().expect("HIR validates");
            document
        })
        .expect("SQL analyzes")
    }

    fn root_expression(document: &HirDocument) -> &Expr {
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        &document.queries[root.query.index()].blocks[0].outputs[0].expr
    }

    fn root_subquery(document: &HirDocument) -> &SubqueryExpr {
        match root_expression(document) {
            Expr::Subquery(subquery) => subquery,
            Expr::Binary { rhs, .. } => {
                let Expr::Subquery(subquery) = rhs.as_ref() else {
                    panic!("binary RHS is a subquery");
                };
                subquery
            }
            _ => panic!("root expression contains the expected subquery"),
        }
    }

    #[test]
    fn scalar_row_and_exists_destinations_install_matching_bindings() {
        for sql in [
            "SELECT (SELECT 1)",
            "SELECT (1, 2) = (SELECT 3, 4)",
            "SELECT EXISTS (SELECT 1)",
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let subquery = root_subquery(&document);
            let mut program = program();
            let prepared = prepare_subquery(&mut program, &document, subquery)
                .expect("subquery destination prepares");

            match (&prepared.destination, subquery) {
                (
                    QueryDestination::RowValueSubqueryResult {
                        result_reg_start,
                        num_regs,
                    },
                    SubqueryExpr::Scalar { .. } | SubqueryExpr::Row { .. },
                ) => assert_eq!(
                    program.subquery_binding(prepared.query),
                    Some(SubqueryBinding::RowValue {
                        start: *result_reg_start,
                        count: *num_regs,
                    })
                ),
                (
                    QueryDestination::ExistsSubqueryResult { result_reg },
                    SubqueryExpr::Exists(_),
                ) => assert_eq!(
                    program.subquery_binding(prepared.query),
                    Some(SubqueryBinding::Exists {
                        register: *result_reg,
                    })
                ),
                _ => panic!("subquery kind has its matching destination"),
            }
            assert!(!prepared.correlated);
        }
    }

    #[test]
    fn in_destination_uses_resolved_comparison_metadata() {
        let document = analyze_sql(
            Schema::new(),
            "SELECT ('a' COLLATE nocase, 2) IN (SELECT 'A', 3)",
        );
        let subquery = root_subquery(&document);
        let SubqueryExpr::In {
            comparison, query, ..
        } = subquery
        else {
            panic!("root is an IN subquery");
        };
        let expected_affinity = comparison
            .components
            .iter()
            .map(|component| component.affinity.aff_mask())
            .collect::<String>();
        let mut program = program();
        let prepared =
            prepare_subquery(&mut program, &document, subquery).expect("IN destination prepares");

        let QueryDestination::EphemeralIndex {
            cursor_id,
            index,
            affinity_str: Some(affinity_str),
            is_delete: false,
        } = &prepared.destination
        else {
            panic!("IN uses an ephemeral index");
        };
        assert_eq!(affinity_str.as_str(), expected_affinity);
        assert_eq!(index.columns.len(), comparison.components.len());
        for (column, component) in index.columns.iter().zip(&comparison.components) {
            assert_eq!(
                column.collation,
                component
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
            );
        }
        assert_eq!(
            program.subquery_binding(*query),
            Some(SubqueryBinding::InIndex { cursor: *cursor_id })
        );
    }

    #[test]
    fn correlation_comes_from_the_resolved_query_capture_set() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let document = analyze_sql(schema, "SELECT (SELECT value) FROM items");
        let subquery = root_subquery(&document);
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, subquery)
            .expect("correlated subquery destination prepares");

        assert!(prepared.correlated);
        assert!(!document
            .query(prepared.query)
            .expect("subquery exists")
            .captures
            .is_empty());
    }

    #[test]
    fn subquery_wrapper_keeps_legacy_initialization_and_body_order() {
        for sql in [
            "SELECT (1, 2) = (SELECT 3, 4)",
            "SELECT EXISTS (SELECT 1)",
            "SELECT 1 IN (SELECT 2)",
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let subquery = root_subquery(&document);
            let mut program = program();
            let prepared = prepare_subquery(&mut program, &document, subquery)
                .expect("subquery destination prepares");
            let marker = program.alloc_register();
            emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
                assert_eq!(query, prepared.query);
                assert!(std::ptr::eq(destination, &prepared.destination));
                program.emit_insn(Insn::Integer {
                    value: 99,
                    dest: marker,
                });
                Ok(())
            })
            .expect("subquery wrapper emits");

            assert!(matches!(
                program.insns.first(),
                Some((Insn::Once { .. }, _))
            ));
            let marker_position = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(insn, Insn::Integer { value: 99, dest } if *dest == marker)
                })
                .expect("body marker was emitted");
            match &prepared.destination {
                QueryDestination::RowValueSubqueryResult {
                    result_reg_start,
                    num_regs,
                } => {
                    assert!(matches!(
                        program.insns.get(1),
                        Some((Insn::BeginSubrtn { .. }, _))
                    ));
                    assert_eq!(marker_position, 2 + num_regs);
                    for (offset, register) in
                        (*result_reg_start..*result_reg_start + *num_regs).enumerate()
                    {
                        assert!(matches!(
                            program.insns.get(2 + offset),
                            Some((Insn::Null { dest, dest_end: None }, _)) if *dest == register
                        ));
                    }
                    assert!(matches!(
                        program.insns.get(marker_position + 1),
                        Some((Insn::Return { .. }, _))
                    ));
                }
                QueryDestination::ExistsSubqueryResult { result_reg } => {
                    assert!(matches!(
                        program.insns.as_slice(),
                        [
                            (Insn::Once { .. }, _),
                            (Insn::BeginSubrtn { .. }, _),
                            (Insn::Integer { value: 0, dest }, _),
                            (Insn::Integer { value: 99, .. }, _),
                            (Insn::Return { .. }, _),
                        ] if *dest == *result_reg
                    ));
                }
                QueryDestination::EphemeralIndex { cursor_id, .. } => {
                    assert!(matches!(
                        program.insns.as_slice(),
                        [
                            (Insn::Once { .. }, _),
                            (Insn::OpenEphemeral { cursor_id: opened, is_table: false }, _),
                            (Insn::Integer { value: 99, .. }, _),
                        ] if *opened == *cursor_id
                    ));
                }
                _ => panic!("expression subquery has a supported destination"),
            }
        }
    }

    #[test]
    fn correlated_subquery_wrapper_does_not_emit_once() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let document = analyze_sql(schema, "SELECT (SELECT value) FROM items");
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
            .expect("correlated subquery destination prepares");

        emit_prepared_subquery(&mut program, &prepared, |_, _, _| Ok(()))
            .expect("correlated wrapper emits");

        assert!(prepared.correlated);
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Once { .. })));
    }

    #[test]
    fn subquery_wrapper_preserves_scalar_and_list_eqp_nodes() {
        for (sql, list) in [
            ("SELECT (SELECT 1)", false),
            ("SELECT 1 IN (SELECT 2)", true),
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let mut program = program_with_mode(QueryMode::ExplainQueryPlan {
                format: turso_parser::ast::EqpFormat::Text,
            });
            let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
                .expect("subquery destination prepares");
            emit_prepared_subquery(&mut program, &prepared, |_, _, _| Ok(()))
                .expect("subquery wrapper emits");

            assert!(matches!(
                program.insns.first(),
                Some((
                    Insn::Explain { detail, .. },
                    _
                )) if matches!(
                    detail.as_ref(),
                    EqpDetail::ListSubquery { correlated: false, .. } if list
                ) || matches!(
                    detail.as_ref(),
                    EqpDetail::ScalarSubquery { correlated: false, .. } if !list
                )
            ));
        }
    }
}
