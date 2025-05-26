use limbo_sqlite3_parser::ast;

use crate::{
    model::query::to_sql::{ToSqlContext, ToSqlString},
    SimulatorEnv,
};

impl ToSqlContext for SimulatorEnv {
    // Id in this case corresponds to index
    fn get_table_name(&self, id: ast::TableInternalId) -> &str {
        let idx: usize = id.into();
        &self.tables[idx].name
    }
}

impl ToSqlString<SimulatorEnv> for ast::Select {
    fn to_sql_string(&self, context: &SimulatorEnv) -> String {
        // TODO: ignore CTE's for now
        let mut ret = String::new();
        ret
    }
}

impl ToSqlString<SimulatorEnv> for ast::SelectBody {
    fn to_sql_string(&self, context: &SimulatorEnv) -> String {
        let mut ret = String::new();
        ret
    }
}

// impl ToSqlString<SimulatorEnv> for ast::OneSelect {
//     fn to_sql_string(&self, context: &SimulatorEnv) -> String {

//     }
// }
