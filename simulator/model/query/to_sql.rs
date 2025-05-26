use limbo_sqlite3_parser::ast::TableInternalId;

pub trait ToSqlContext {
    // fn get_table_id(&self, tbl_name: &str) -> TableInternalId;
    // TODO: for now assume id exists in the context
    fn get_table_name(&self, id: TableInternalId) -> &str;
}

pub trait ToSqlString<C: ToSqlContext> {
    fn to_sql_string(&self, context: &C) -> String;
}
