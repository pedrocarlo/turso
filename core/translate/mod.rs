//! The VDBE bytecode code generator.
//!
//! This module is responsible for translating the SQL AST into a sequence of
//! instructions for the VDBE. The VDBE is a register-based virtual machine that
//! executes bytecode instructions. This code generator is responsible for taking
//! the SQL AST and generating the corresponding VDBE instructions. For example,
//! a SELECT statement will be translated into a sequence of instructions that
//! will read rows from the database and filter them according to a WHERE clause.

pub(crate) mod aggregation;
pub(crate) mod alter;
pub(crate) mod delete;
pub(crate) mod emitter;
pub(crate) mod expr;
pub(crate) mod group_by;
pub(crate) mod index;
pub(crate) mod insert;
pub(crate) mod main_loop;
pub(crate) mod optimizer;
pub(crate) mod order_by;
pub(crate) mod plan;
pub(crate) mod planner;
pub(crate) mod pragma;
pub(crate) mod result_row;
pub(crate) mod schema;
pub(crate) mod select;
pub(crate) mod subquery;
pub(crate) mod transaction;
pub(crate) mod update;

use crate::fast_lock::SpinLock;
use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::translate::delete::translate_delete;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::Program;
use crate::{bail_parse_error, Connection, Result, SymbolTable};
use alter::translate_alter_table;
use fallible_iterator::FallibleIterator as _;
use index::translate_create_index;
use insert::translate_insert;
use limbo_sqlite3_parser::ast::{self, Cmd, Delete, Insert};
use limbo_sqlite3_parser::lexer::sql::Parser;
use schema::{translate_create_table, translate_create_virtual_table, translate_drop_table};
use select::translate_select;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use transaction::{translate_tx_begin, translate_tx_commit};
use update::translate_update;

struct TranslateArgs<'a> {
    schema: &'a Schema,
    stmt: ast::Stmt,
    database_header: Arc<SpinLock<DatabaseHeader>>,
    pager: Rc<Pager>,
    syms: &'a SymbolTable,
    query_mode: QueryMode,
}

#[doc(hidden)]
pub struct DeepParseArgs<'a> {
    schema: &'a Schema,
    database_header: Arc<SpinLock<DatabaseHeader>>,
    pager: Rc<Pager>,
    syms: &'a SymbolTable,
    query_mode: QueryMode,
}

/// Translate SQL statement into bytecode program.
pub fn translate(
    schema: &Schema,
    stmt: ast::Stmt,
    database_header: Arc<SpinLock<DatabaseHeader>>,
    pager: Rc<Pager>,
    connection: Weak<Connection>,
    syms: &SymbolTable,
    query_mode: QueryMode,
) -> Result<Program> {
    let args = TranslateArgs {
        schema,
        stmt,
        database_header: database_header.clone(),
        pager,
        syms,
        query_mode,
    };
    let program = translate_inner(args, None)?;
    Ok(program.build(database_header, connection))
}

/// Inner translate to allow Deep Parsing of SqlStatements
fn translate_inner(args: TranslateArgs, program: Option<ProgramBuilder>) -> Result<ProgramBuilder> {
    let TranslateArgs {
        schema,
        stmt,
        database_header,
        pager,
        syms,
        query_mode,
    } = args;

    let program = match stmt {
        ast::Stmt::AlterTable(alter_table) => {
            let (tbl_name, body) = *alter_table;
            translate_alter_table(
                DeepParseArgs {
                    schema,
                    database_header,
                    pager,
                    syms,
                    query_mode,
                },
                tbl_name,
                body,
                program,
            )?
        }
        ast::Stmt::Analyze(_) => bail_parse_error!("ANALYZE not supported yet"),
        ast::Stmt::Attach { .. } => bail_parse_error!("ATTACH not supported yet"),
        ast::Stmt::Begin(tx_type, tx_name) => translate_tx_begin(tx_type, tx_name, program)?,
        ast::Stmt::Commit(tx_name) => translate_tx_commit(tx_name, program)?,
        ast::Stmt::CreateIndex {
            unique,
            if_not_exists,
            idx_name,
            tbl_name,
            columns,
            ..
        } => {
            let mut program = translate_create_index(
                query_mode,
                (unique, if_not_exists),
                &idx_name.name.0,
                &tbl_name.0,
                &columns,
                schema,
                program,
            )?;
            program.change_count_on = true;
            program
        }
        ast::Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        } => translate_create_table(
            query_mode,
            tbl_name,
            temporary,
            *body,
            if_not_exists,
            schema,
            program,
        )?,
        ast::Stmt::CreateTrigger { .. } => bail_parse_error!("CREATE TRIGGER not supported yet"),
        ast::Stmt::CreateView { .. } => bail_parse_error!("CREATE VIEW not supported yet"),
        ast::Stmt::CreateVirtualTable(vtab) => {
            translate_create_virtual_table(*vtab, schema, query_mode, program)?
        }
        ast::Stmt::Delete(delete) => {
            let Delete {
                tbl_name,
                where_clause,
                limit,
                ..
            } = *delete;
            let mut program = translate_delete(
                query_mode,
                schema,
                &tbl_name,
                where_clause,
                limit,
                syms,
                program,
            )?;
            program.change_count_on = true;
            program
        }
        ast::Stmt::Detach(_) => bail_parse_error!("DETACH not supported yet"),
        ast::Stmt::DropIndex { .. } => bail_parse_error!("DROP INDEX not supported yet"),
        ast::Stmt::DropTable {
            if_exists,
            tbl_name,
        } => translate_drop_table(query_mode, tbl_name, if_exists, schema, program)?,
        ast::Stmt::DropTrigger { .. } => bail_parse_error!("DROP TRIGGER not supported yet"),
        ast::Stmt::DropView { .. } => bail_parse_error!("DROP VIEW not supported yet"),
        ast::Stmt::Pragma(name, body) => pragma::translate_pragma(
            query_mode,
            schema,
            &name,
            body.map(|b| *b),
            database_header.clone(),
            pager,
            program,
        )?,
        ast::Stmt::Reindex { .. } => bail_parse_error!("REINDEX not supported yet"),
        ast::Stmt::Release(_) => bail_parse_error!("RELEASE not supported yet"),
        ast::Stmt::Rollback { .. } => bail_parse_error!("ROLLBACK not supported yet"),
        ast::Stmt::Savepoint(_) => bail_parse_error!("SAVEPOINT not supported yet"),
        ast::Stmt::Select(select) => translate_select(query_mode, schema, *select, syms, program)?,
        ast::Stmt::Update(mut update) => {
            translate_update(query_mode, schema, &mut update, syms, program)?
        }
        ast::Stmt::Vacuum(_, _) => bail_parse_error!("VACUUM not supported yet"),
        ast::Stmt::Insert(insert) => {
            let Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            } = *insert;
            let mut program = translate_insert(
                query_mode,
                schema,
                &with,
                &or_conflict,
                &tbl_name,
                &columns,
                &body,
                &returning,
                syms,
                program,
            )?;
            program.change_count_on = true;
            program
        }
    };
    Ok(program)
}

// Sql Formatted string
fn deep_parse(
    args: DeepParseArgs,
    mut program: ProgramBuilder,
    sql: String,
) -> Result<ProgramBuilder> {
    tracing::debug!("Deep Parse: {}", sql);
    let mut parser = Parser::new(sql.as_bytes());
    let cmd = parser.next()?;
    let DeepParseArgs {
        schema,
        database_header,
        pager,
        syms,
        query_mode,
    } = args;
    program.nested += 1;
    if let Some(Cmd::Stmt(stmt)) = cmd {
        program = translate_inner(
            TranslateArgs {
                schema,
                stmt,
                database_header,
                pager,
                syms,
                query_mode,
            },
            Some(program),
        )?;
        program.nested -= 1;
        Ok(program)
    } else {
        // There should be no explain queries here
        panic!("Incorrect deep parse query: {}", sql)
    }
}
