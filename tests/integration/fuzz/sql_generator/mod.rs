mod context;
mod iterator;

#[derive(Debug, Clone, Copy)]
/// Token is an attempt of flat representation of all possible Sql values
pub enum Token {
    None, // Serves the same concept as in Option
    // Placeholder Token
    Expr, // General expression
    ExprList,
    Literal, // Literal Value
    ResultColumn,
    ColumnName,
    ColumnAlias,
    SchemaName,
    TableName,
    TableAlias,

    Select,
    All,
    Distinct,
    Star,
    // Predicate
    // TODO have more predicates
    And,
    Or,
    Eq,
    Neq,
    Gt,
    Lt,
    Ge,
    Le,
    Like,
    Glob,
    Regexp,
    Match,

    From,
    // TODO: schema-name
    As,
    // TODO: Indexed by statements
    // TODO: Table function
    // TODO: Joins and subquerys
    // TODO: Where
    // TODO: Group by
    // TODO: Window
    // TODO: Values
    // TODO: compound operators
    // TODO: Order by
    // TODO: Limit
    Variable,
    UnaryOperator,
    BinaryOperator,
    Function,
    // TODO: some type of expression list
    Cast,
    TypeName,
    Collate, // kind of unary operator
    CollationName,
    Exists,
    Not,
    Case,
    // TODO: raise function
    Escape,
    IsNull,
    NotNull,
    Null,
    Is,
    Between,
    In,
    TableFunction,
    WhenThen, // Present in Case statement
    Else,
    End,
}

pub trait ToTokens {
    fn to_tokens(&self) -> Vec<Token>;
}

#[cfg(test)]
mod tests {
    use super::context::ExprContext;

    use limbo_sim_lib::generation::Arbitrary;

    #[test]
    fn random_expr() {
        let mut rng = rand::rng();
        let ctx = ExprContext::arbitrary(&mut rng);
        println!("Context: {:?}", ctx);
        let x = ctx.eval(&mut rng);
        dbg!(&x);
    }
}
