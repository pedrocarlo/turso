use limbo_ext::VTabKind;
use limbo_sqlite3_parser::ast::{AlterTableBody, Name, QualifiedName};

use crate::{
    bail_parse_error,
    schema::Table,
    vdbe::builder::{ProgramBuilder, ProgramBuilderOpts},
    Result,
};

use super::{deep_parse, schema::SQLITE_TABLEID, DeepParseArgs};

pub fn translate_alter_table(
    args: DeepParseArgs,
    tbl_name: QualifiedName,
    body: AlterTableBody,
    program: Option<ProgramBuilder>,
) -> Result<ProgramBuilder> {
    match body {
        AlterTableBody::RenameTo(new_tbl_name) => {
            translate_alter_table_rename_to(args, tbl_name, new_tbl_name, program)
        }
        _ => bail_parse_error!("Only RENAME TO implemented for ALTER TABLE"),
    }
}

fn translate_alter_table_rename_to(
    args: DeepParseArgs,
    tbl_name: QualifiedName,
    new_tbl_name: Name,
    program: Option<ProgramBuilder>,
) -> Result<ProgramBuilder> {
    let mut program = program.unwrap_or(ProgramBuilder::new(ProgramBuilderOpts {
        query_mode: args.query_mode,
        num_cursors: 1,
        approx_num_insns: 0,  // TODO
        approx_num_labels: 0, // TODO
    }));

    let table_name = tbl_name.name.0.as_str();

    // to locate the Table SQLite is a bit more elaborate on how it searches for the table
    // right now we are just defaulting to searching in the schema
    // https://github.com/sqlite/sqlite/blob/master/src/build.c#L471
    let table = args.schema.get_table(table_name);
    if table.is_none() {
        bail_parse_error!("No such table: {}", table_name);
    }
    // SAFE: Checked above if table is none
    let table = table.unwrap();

    /* START - CHECK TABLE NAME */
    // TODO see difference between what a Shadow Table name is for Virtual Tables
    // Check that a table, index or virtual table named as 'new_table_name' does not already exist

    let new_table_name = new_tbl_name.0.as_str();
    // Checks for tables and virtual tables
    if args.schema.get_table(new_table_name).is_some() {
        bail_parse_error!(
            "there is already another table or index with this name: {}",
            new_table_name
        );
    }
    let indices = args.schema.get_indices(new_table_name);
    for index in indices {
        if index.name == new_table_name {
            bail_parse_error!(
                "there is already another table or index with this name: {}",
                new_table_name
            );
        }
    }
    /* END - CHECK TABLE NAME */

    /* START - CHECK SYSTEM OR RESERVE TABLE */

    // Make sure it is not a system table being altered, or a reserved name
    // that the table is being renamed to.

    if !is_alterable_table(&table, table_name) {
        bail_parse_error!("table {} may not be altered", table_name);
    }
    // TODO: SQLite does a separate object name check here

    // TODO: When VIEWs are implemented, should bail here

    /* END - CHECK SYSTEM OR RESERVE TABLE */

    // TODO: when we support many databases, edit this to reference the sqlite_schema from that database only
    let _db = "0";
    // TODO: when we support temp table update this value
    let _is_from_temp_db = 0;

    program.emit_transaction(true);

    /* TODO: RENAME REFERENCES TO TABLE
     * Rewrite all CREATE TABLE, INDEX, TRIGGER or VIEW statements in
     * the schema to use the new table name. */

    // TODO: implement sqlite_rename_table when we support foreign keys

    // let sql = format!(
    //     "UPDATE {} SET sql = sqlite_rename_table({}, type, name, sql, {}, {}, {})
    //     WHERE (type!='index' OR tbl_name={} COLLATE nocase)
    //     AND name NOT LIKE 'sqliteX_%%' ESCAPE 'X'",
    //     SQLITE_TABLEID, db, table_name, new_table_name, is_from_temp_db, table_name
    // );

    // program = deep_parse(args, program, sql)?;

    /* EXECUTE SQL Staments to rename table.
     * Update the tbl_name and name columns of the sqlite_schema table as required.
     */

    let sql = format!(
        "UPDATE {} SET tbl_name = {}, 
        name = CASE 
            WHEN type='table' THEN {} 
            WHEN name LIKE 'sqliteX_autoindex%%' ESCAPE 'X' 
                AND type='index' THEN 
            'sqlite_autoindex_' || {} || substr(name,{}+18) 
            ELSE name END 
        WHERE tbl_name={} COLLATE nocase AND 
            (type='table' OR type='index' OR type='trigger');",
        SQLITE_TABLEID,
        new_table_name,
        new_table_name,
        new_table_name,
        table_name.len(),
        table_name
    );
    program = deep_parse(args, program, sql)?;

    /* TODO: If the sqlite_sequence table exists in this database, then update
     * it with the new table name. */

    /* TODO: If the table being renamed is not itself part of the temp database,
     * edit view and trigger definitions within the temp database
     * as required. */

    /* TODO: If this is a virtual table, invoke the xRename() function if
     * one is defined. The xRename() callback will modify the names
     * of any resources used by the v-table implementation (including other
     * SQLite tables) that are identified by the name of the virtual table.
     */

    Ok(program)
}

// TODO: Currently we only have sqlite3_schema system table. When we add
// more system_tables, we need to add them here as well
fn is_alterable_table(table: &Table, tbl_name: &str) -> bool {
    let virtual_table = table.virtual_table();
    if let Some(vtab) = virtual_table {
        if matches!(vtab.kind, VTabKind::TableValuedFunction) {
            return false;
        }
    }
    // TODO: if read-only SHADOW TABLE return false

    // Sqlite reserved table
    if tbl_name.starts_with("sqlite_") {
        return false;
    }

    true
}
