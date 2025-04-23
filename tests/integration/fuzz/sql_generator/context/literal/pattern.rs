use crate::fuzz::sql_generator::{
    context::{Neighbour, PatternContext},
    Token,
};

impl Neighbour for PatternContext {
    fn neighbours(&self, token_idx: usize, token: Token) -> Vec<Token> {
        if matches!(token, Token::Expr) && token_idx == 0 {
            return vec![
                Token::Not,
                Token::Like,
                Token::Glob,
                Token::Regexp,
                Token::Match,
            ];
        }
        if matches!(token, Token::Not) && token_idx == 1 {
            return vec![Token::Like, Token::Glob, Token::Regexp, Token::Match];
        }
        match self {
            PatternContext::Like => Self::like(token_idx, token),
            PatternContext::Rest => Self::rest(token_idx, token),
        }
    }
}

impl PatternContext {
    fn like(token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Expr if token_idx == 1 => {
                vec![Token::None, Token::Escape]
            }
            Token::Escape => {
                vec![Token::Expr]
            }
            Token::Expr if token_idx == 3 => {
                vec![]
            }

            _ => unreachable!(),
        }
    }

    fn rest(_token_idx: usize, token: Token) -> Vec<Token> {
        match token {
            Token::Glob | Token::Regexp | Token::Match => {
                vec![Token::Expr]
            }
            Token::Expr => {
                vec![]
            }
            _ => unreachable!(),
        }
    }
}
