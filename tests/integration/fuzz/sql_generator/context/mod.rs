use super::Token;

mod literal;

trait Neighbour {
    fn neighbours(&self, token_idx: usize, token: Token) -> Vec<Token>;
}

pub enum Context {
    ResultColumn,
}

pub enum ResultColumnContext {
    Expr,
    Table,
}

#[derive(Debug, Clone, Copy)]
pub enum ExprContext {
    SchemaName,
    BinaryOperator,
    Function,
    ExprList,
    Cast,
    Collate,
    Pattern(PatternContext), // Like, Glob, Regexp, Match
    Null,
    Is,
    Between,
    In,
    Exists,
    Case,
}

#[derive(Debug, Clone, Copy)]
pub enum PatternContext {
    Like,
    Rest,
}
