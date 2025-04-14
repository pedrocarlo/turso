use limbo_ext::VTabKind;
use limbo_sqlite3_parser::ast::{AlterTableBody, Name, QualifiedName};

use crate::{
    bail_parse_error,
    schema::{Schema, Table},
    vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode},
    Result, SymbolTable,
};

pub fn translate_alter_table(
    tbl_name: QualifiedName,
    body: AlterTableBody,
    schema: &Schema,
    syms: &SymbolTable,
    query_mode: QueryMode,
) -> Result<ProgramBuilder> {
    match body {
        AlterTableBody::RenameTo(new_tbl_name) => {
            translate_alter_table_rename_to(tbl_name, new_tbl_name, schema, syms, query_mode)
        }
        _ => bail_parse_error!("Only RENAME TO implemented for ALTER TABLE"),
    }
}

fn translate_alter_table_rename_to(
    tbl_name: QualifiedName,
    new_tbl_name: Name,
    schema: &Schema,
    syms: &SymbolTable,
    query_mode: QueryMode,
) -> Result<ProgramBuilder> {
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 1,
        approx_num_insns: 0,  // TODO
        approx_num_labels: 0, // TODO
    });

    // TODO: use TRANSACTIONS for this type of operation

    // to locate the Table SQLite is a bit more elaborate on how it searches for the table
    // right now we are just defaulting to searching in the schema
    // https://github.com/sqlite/sqlite/blob/master/src/build.c#L471
    let table = schema.get_table(tbl_name.name.0.as_str());
    if table.is_none() {
        bail_parse_error!("No such table: {}", tbl_name.name.0.as_str());
    }
    // SAFE: Checked above if table is none
    let table = table.unwrap();

    /* START CHECK TABLE NAME */
    // TODO see difference between what a Shadow Table name is for Virtual Tables
    // Check that a table, index or virtual table named as 'new_tbl_name' does not already exist

    let table_key_name = new_tbl_name.0.as_str();
    // Checks for tables and virtual tables
    if schema.get_table(table_key_name).is_some() {
        bail_parse_error!(
            "there is already another table or index with this name: {}",
            table_key_name
        );
    }
    let indices = schema.get_indices(table_key_name);
    for index in indices {
        if index.name == table_key_name {
            bail_parse_error!(
                "there is already another table or index with this name: {}",
                table_key_name
            );
        }
    }
    /* END CHECK TABLE NAME */

    /* START CHECK SYSTEM OR RESERVE TABLE */
    // Make sure it is not a system table being altered, or a reserved name
    // that the table is being renamed to.

    if !is_alterable_table(&table, tbl_name.name.0.as_str()) {
        bail_parse_error!("table {} may not be altered", tbl_name.name.0.as_str());
    }
    // TODO: SQLite does a separate object name check here

    // TODO: When VIEWs are implemented, should bail here

    /* END CHECK SYSTEM OR RESERVE TABLE */

    // EXECUTE SQL Staments to rename table

    Ok(program)
}

// TODO: Currently we only have sqlite3_schema system table. When we add
// more types we with maybe different names
fn is_alterable_table(table: &Table, tbl_name: &str) -> bool {
    let virtual_table = table.virtual_table();
    if let Some(vtab) = virtual_table {
        if matches!(vtab.kind, VTabKind::TableValuedFunction) {
            return false;
        }
    }
    // TODO if read-only SHADOW TABLE return false

    // Sqlite reserved table
    if tbl_name.starts_with("sqlite_") {
        return false;
    }

    true
}
