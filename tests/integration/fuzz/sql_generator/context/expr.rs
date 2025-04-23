use limbo_sim_lib::generation::Arbitrary;
use rand::seq::IndexedRandom;

use crate::fuzz::sql_generator::Token;

use super::{ExprContext, Neighbour};

impl ExprContext {
    pub fn eval<R: rand::Rng>(&self, rng: &mut R) -> Vec<Token> {
        let mut tokens = Vec::with_capacity(20);
        let mut curr = self.start();
        let mut idx = 0;
        while let Some(tok) = curr.choose(rng) {
            tokens.push(*tok);
            curr = self.neighbours(idx, *tok);
            idx += 1;
        }
        tokens
    }
}

impl Arbitrary for ExprContext {
    fn arbitrary<R: rand::Rng>(rng: &mut R) -> Self {
        match rng.random_range(0..14) {
            0 => ExprContext::SchemaName,
            1 => ExprContext::UnaryOperator,
            2 => ExprContext::BinaryOperator,
            3 => ExprContext::Function,
            4 => ExprContext::ExprList,
            5 => ExprContext::Cast,
            6 => ExprContext::Collate,
            7 => ExprContext::LikePattern,
            8 => ExprContext::OtherPattern,
            9 => ExprContext::Null,
            10 => ExprContext::Is,
            11 => ExprContext::Between,
            12 => ExprContext::In,
            13 => ExprContext::Exists,
            14 => ExprContext::Case,
            _ => unreachable!(),
        }
    }
}

// TODO: bind parameter
impl Neighbour for ExprContext {
    fn neighbours(&self, token_idx: usize, token: Token) -> Vec<Token> {
        match self {
            ExprContext::SchemaName => Self::schema_name(token_idx, token),
            ExprContext::UnaryOperator => Self::unary_operator(token_idx, token),
            ExprContext::BinaryOperator => Self::binary_operator(token_idx, token),
            ExprContext::Function => Self::function(token_idx, token),
            ExprContext::ExprList => Self::expr_list(token_idx, token),
            ExprContext::Cast => Self::cast(token_idx, token),
            ExprContext::Collate => Self::collate(token_idx, token),
            ExprContext::LikePattern => Self::like_pattern(token_idx, token),
            ExprContext::OtherPattern => Self::rest_pattern(token_idx, token),
            ExprContext::Null => Self::null(token_idx, token),
            ExprContext::Is => Self::is(token_idx, token),
            ExprContext::Between => Self::between(token_idx, token),
            ExprContext::In => Self::in_ctx(token_idx, token),
            ExprContext::Exists => Self::exists(token_idx, token),
            ExprContext::Case => Self::case(token_idx, token),
        }
    }

    fn start(&self) -> Vec<Token> {
        match self {
            ExprContext::SchemaName => vec![Token::SchemaName, Token::TableName, Token::ColumnName],
            ExprContext::UnaryOperator => vec![Token::UnaryOperator],
            ExprContext::BinaryOperator => vec![Token::Expr],
            ExprContext::Function => vec![Token::Function],
            ExprContext::ExprList => vec![Token::ExprList],
            ExprContext::Cast => vec![Token::Cast],
            ExprContext::Collate => vec![Token::Expr],
            ExprContext::LikePattern | ExprContext::OtherPattern => vec![Token::Expr],
            ExprContext::Null => vec![Token::Expr],
            ExprContext::Is => vec![Token::Expr],
            ExprContext::Between => vec![Token::Expr],
            ExprContext::In => vec![Token::Expr],
            ExprContext::Exists => vec![Token::Not, Token::Exists, Token::Select],
            ExprContext::Case => vec![Token::Case],
        }
    }
}

impl ExprContext {
    fn schema_name(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::SchemaName => vec![Token::TableName],
            Token::TableName => vec![Token::ColumnName],
            Token::ColumnName => vec![],
            _ => unreachable!(),
        }
    }

    fn unary_operator(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::UnaryOperator => vec![Token::Expr],
            Token::Expr => vec![],
            _ => unreachable!(),
        }
    }

    fn binary_operator(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr if token_idx == 0 => vec![Token::BinaryOperator],
            Token::BinaryOperator => vec![Token::Expr],
            Token::Expr if token_idx == 2 => vec![],
            _ => unreachable!(),
        }
    }

    fn function(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            // TODO: filter clause and over clause
            Token::Function => vec![Token::None],
            Token::None => vec![],
            _ => unreachable!(),
        }
    }

    fn expr_list(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::ExprList => vec![],
            // Token::Expr => {
            //     vec![Token::Expr, Token::None]
            // }
            _ => unreachable!(),
        }
    }

    fn cast(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Cast => vec![Token::Expr],
            Token::Expr => vec![Token::As],
            Token::As => vec![Token::TypeName],
            Token::TypeName => vec![],
            _ => unreachable!(),
        }
    }

    fn collate(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr => vec![Token::Collate],
            Token::Collate => vec![Token::CollationName],
            Token::CollationName => vec![],
            _ => unreachable!(),
        }
    }

    fn like_pattern(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr if token_idx == 0 => vec![Token::Not, Token::Like],
            Token::Not => vec![Token::Like],
            Token::Like => vec![Token::Expr],
            Token::Expr if token_idx == 3 || token_idx == 2 => vec![Token::None, Token::Escape],
            Token::Escape => vec![Token::Expr],
            Token::Expr | Token::None => vec![],
            _ => unreachable!(),
        }
    }

    fn rest_pattern(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr if token_idx == 0 => {
                vec![Token::Not, Token::Glob, Token::Regexp, Token::Match]
            }
            Token::Not => vec![Token::Glob, Token::Regexp, Token::Match],
            Token::Glob | Token::Regexp | Token::Match => vec![Token::Expr],
            Token::Expr => vec![],
            _ => unreachable!(),
        }
    }

    fn null(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr => vec![Token::IsNull, Token::NotNull, Token::Not],
            Token::IsNull | Token::NotNull | Token::Null => vec![],
            Token::Not => vec![Token::Null],
            _ => unreachable!(),
        }
    }

    fn is(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr if token_idx == 0 => vec![Token::Is],
            Token::Is => vec![Token::Distinct, Token::Not, Token::Expr],
            Token::Not => vec![Token::Distinct, Token::Expr],
            Token::Distinct => vec![Token::From],
            Token::From => vec![Token::Expr],
            Token::Expr => vec![],
            _ => unreachable!(),
        }
    }

    fn between(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr if token_idx == 0 => vec![Token::Not, Token::Between],
            Token::Not => vec![Token::Between],
            Token::Between => vec![Token::Expr],
            Token::Expr if token_idx == 2 || token_idx == 3 => vec![Token::And],
            Token::And => vec![Token::Expr],
            Token::Expr => vec![],
            _ => unreachable!(),
        }
    }

    fn in_ctx(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr => vec![Token::Not, Token::In],
            Token::Not => vec![Token::In],
            // TODO: select stmt + expr list
            Token::In => vec![Token::SchemaName, Token::TableName, Token::TableFunction],
            Token::SchemaName => vec![Token::TableName, Token::TableFunction],
            Token::TableName | Token::TableFunction => vec![],
            _ => unreachable!(),
        }
    }

    fn exists(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Not => vec![Token::Exists],
            Token::Exists => vec![Token::Select],
            Token::Select => vec![],
            _ => unreachable!(),
        }
    }

    fn case(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Case => vec![Token::Expr, Token::WhenThen],
            Token::Expr if token_idx == 1 => vec![Token::WhenThen],
            Token::WhenThen => vec![Token::WhenThen, Token::Else, Token::End],
            Token::Else => vec![Token::Expr],
            Token::Expr => vec![Token::End],
            Token::End => vec![],
            _ => unreachable!(),
        }
    }
}
