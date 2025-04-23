mod pattern;

use crate::fuzz::sql_generator::Token;

use super::{ExprContext, Neighbour, PatternContext};

impl Neighbour for ExprContext {
    fn neighbours(&self, token_idx: usize, token: Token) -> Vec<Token> {
        match self {
            ExprContext::SchemaName => Self::schema_name(token_idx, token),
            ExprContext::BinaryOperator => Self::binary_operator(token_idx, token),
            ExprContext::Function => Self::function(token_idx, token),
            ExprContext::ExprList => Self::expr_list(token_idx, token),
            ExprContext::Cast => Self::cast(token_idx, token),
            ExprContext::Collate => Self::collate(token_idx, token),
            ExprContext::Pattern(pattern_ctx) => {
                Self::pattern(pattern_ctx.clone(), token_idx, token)
            }
            ExprContext::Null => Self::null(token_idx, token),
            ExprContext::Is => Self::is(token_idx, token),
            ExprContext::Between => Self::between(token_idx, token),
            ExprContext::In => Self::in_ctx(token_idx, token),
            ExprContext::Exists => Self::exists(token_idx, token),
            ExprContext::Case => Self::case(token_idx, token),
        }
    }
}

impl ExprContext {
    fn schema_name(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::SchemaName => {
                vec![Token::TableName]
            }
            Token::TableName => {
                vec![Token::ColumnName]
            }
            Token::ColumnName => {
                vec![]
            }
            _ => unreachable!(),
        }
    }

    fn binary_operator(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr if token_idx == 0 => {
                vec![Token::BinaryOperator]
            }
            Token::BinaryOperator => {
                vec![Token::Expr]
            }

            Token::Expr if token_idx == 2 => {
                vec![]
            }
            _ => unreachable!(),
        }
    }

    fn function(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::FunctionName => {
                vec![Token::FunctionArguments]
            }
            Token::FunctionArguments => {
                // TODO: filter clause and over clause
                vec![Token::None]
            }
            _ => unreachable!(),
        }
    }

    fn expr_list(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr => {
                vec![Token::Expr, Token::None]
            }
            _ => unreachable!(),
        }
    }

    fn cast(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Cast => {
                vec![Token::Expr]
            }
            Token::Expr => {
                vec![Token::As]
            }
            Token::As => {
                vec![Token::TypeName]
            }
            _ => unreachable!(),
        }
    }

    fn collate(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr => {
                vec![Token::Collate]
            }
            Token::Collate => {
                vec![Token::CollationName]
            }
            Token::CollationName => {
                vec![]
            }
            _ => unreachable!(),
        }
    }

    fn pattern(ctx: PatternContext, token_idx: usize, token: Token) -> Vec<Token> {
        ctx.neighbours(token_idx, token)
    }

    fn null(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr => {
                vec![Token::IsNull, Token::NotNull, Token::Not]
            }
            Token::IsNull | Token::NotNull | Token::Null => vec![],
            Token::Not => {
                vec![Token::Null]
            }
            _ => unreachable!(),
        }
    }

    fn is(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr if token_idx == 0 => {
                vec![Token::Is]
            }
            Token::Is => {
                vec![Token::Distinct, Token::Not, Token::Expr]
            }
            Token::Not => {
                vec![Token::Distinct, Token::Expr]
            }
            Token::Distinct => {
                vec![Token::From]
            }
            Token::From => {
                vec![Token::Expr]
            }
            Token::Expr => {
                vec![]
            }
            _ => unreachable!(),
        }
    }

    fn between(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr if token_idx == 0 => {
                vec![Token::Not, Token::Between]
            }
            Token::Not => {
                vec![Token::Between]
            }
            Token::Between => {
                vec![Token::Expr]
            }
            Token::Expr if token_idx == 2 || token_idx == 3 => {
                vec![Token::And]
            }
            Token::And => {
                vec![Token::Expr]
            }
            Token::Expr => {
                vec![]
            }
            _ => unreachable!(),
        }
    }

    fn in_ctx(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr => {
                vec![Token::Not, Token::In]
            }
            Token::Not => {
                vec![Token::In]
            }
            Token::In => {
                // TODO: select stmt + expr list
                vec![Token::SchemaName, Token::TableName, Token::TableFunction]
            }
            Token::SchemaName => {
                vec![Token::TableName, Token::TableFunction]
            }
            Token::TableName | Token::TableFunction => {
                vec![]
            }
            _ => unreachable!(),
        }
    }

    fn exists(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Not => {
                vec![Token::Exists]
            }
            Token::Exists => {
                vec![Token::Select]
            }
            Token::Select => {
                vec![]
            }
            _ => unreachable!(),
        }
    }

    fn case(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Case => {
                vec![Token::Expr, Token::WhenThen]
            }
            Token::Expr if token_idx == 1 => {
                vec![Token::WhenThen]
            }
            Token::WhenThen => {
                vec![Token::WhenThen, Token::Else, Token::End]
            }
            Token::Else => {
                vec![Token::Expr]
            }
            Token::Expr => {
                vec![Token::End]
            }
            Token::End => {
                vec![]
            }
            _ => unreachable!(),
        }
    }
}
