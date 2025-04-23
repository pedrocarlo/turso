use super::Token;

mod expr;

pub trait Neighbour {
    fn neighbours(&self, token_idx: usize, token: Token) -> Vec<Token>;
    fn start(&self) -> Vec<Token>;
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
    UnaryOperator,
    BinaryOperator,
    Function,
    ExprList,
    Cast,
    Collate,
    LikePattern,  // Like,
    OtherPattern, // Glob, Regexp, Match
    Null,
    Is,
    Between,
    In,
    Exists,
    Case,
}
