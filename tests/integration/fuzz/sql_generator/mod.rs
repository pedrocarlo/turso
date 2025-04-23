mod context;
mod iterator;

use limbo_sim_lib::model::query::select::{Distinctness, Predicate, ResultColumn};

#[derive(Debug, Clone, Copy)]
/// Token is an attempt of flat representation of all possible Sql values
pub enum Token {
    None, // Serves the same concept as in Option
    // Placeholder Token
    Expr,    // General expression
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
    FunctionName,
    FunctionArguments,
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

impl ToTokens for Distinctness {
    fn to_tokens(&self) -> Vec<Token> {
        let token = match self {
            Distinctness::All => Token::All,
            Distinctness::Distinct => Token::Distinct,
        };
        vec![token]
    }
}

impl ToTokens for ResultColumn {
    fn to_tokens(&self) -> Vec<Token> {
        match self {
            ResultColumn::Star => vec![Token::Star],
            ResultColumn::Column(..) => vec![Token::ColumnName],
            ResultColumn::Expr(predicate) => predicate.to_tokens(),
        }
    }
}

// Predicate currently is a bit limited as it can only compare Columns to Values
// And not Values to Values or Values to Columns
impl ToTokens for Predicate {
    fn to_tokens(&self) -> Vec<Token> {
        let tokens = match self {
            Predicate::And(predicates) => predicates
                .iter()
                .enumerate()
                .flat_map(|(idx, p)| {
                    let mut intermediate = p.to_tokens();

                    if idx % 2 == 1 {
                        intermediate.insert(0, Token::And);
                    }
                    intermediate
                })
                .collect(),
            Predicate::Or(predicates) => predicates
                .iter()
                .enumerate()
                .flat_map(|(idx, p)| {
                    let mut intermediate = p.to_tokens();

                    if idx % 2 == 1 {
                        intermediate.insert(0, Token::Or);
                    }
                    intermediate
                })
                .collect(),
            Predicate::Eq(..) => {
                vec![Token::ColumnName, Token::Eq, Token::Literal]
            }
            Predicate::Neq(..) => {
                vec![Token::ColumnName, Token::Neq, Token::Literal]
            }
            Predicate::Gt(..) => {
                vec![Token::ColumnName, Token::Gt, Token::Literal]
            }
            Predicate::Lt(..) => {
                vec![Token::ColumnName, Token::Lt, Token::Literal]
            }
            Predicate::Ge(..) => {
                vec![Token::ColumnName, Token::Ge, Token::Literal]
            }
            Predicate::Le(..) => {
                vec![Token::ColumnName, Token::Le, Token::Literal]
            }
            Predicate::Like(..) => {
                vec![Token::ColumnName, Token::Like, Token::Literal]
            }
        };
        tokens
    }
}
