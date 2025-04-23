//! This file attempts to represent the possible next tokens of each Token in a select query

use std::{collections::VecDeque, marker::PhantomData};

use super::Token::{self};

#[derive(Debug)]
// Will need more context to generate queries
struct TokenGenerator {
    budget: usize,
    curr_budget: usize,
    complexity: usize,
    curr_complexity: usize,
    token_queue: VecDeque<Token>,
}

impl Iterator for TokenGenerator {
    type Item = Token;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO some addtional checks to make sure that
        if self.curr_budget >= self.budget || self.curr_complexity >= self.complexity {
            let token = self.token_queue.pop_back();
            return token;
        }

        None
    }
}

impl Token {
    fn token_neighbours() {}

    /* Start Result Column Diagram */
    fn result_column_neighbors() -> Vec<Token> {
        vec![Token::TableName, Token::Star, Token::Expr]
    }

    fn result_column_expr_neighbours() -> Vec<Token> {
        vec![]
    }

    /* End Result Column Diagram */

    fn expression_neighbours() -> Vec<Token> {
        vec![
            Token::Literal,
            Token::ColumnName,
            Token::SchemaName,
            Token::UnaryOperator,
            Token::Expr,
            Token::FunctionName,
            Token::Cast,
            Token::Not,
            Token::Exists,
            Token::Select,
            Token::Case,
            // TODO raise function start here
        ]
    }

    fn select_neighbours() -> Vec<Token> {
        vec![Token::All, Token::Distinct, Token::ColumnName]
    }

    fn distinct_neighbours() -> Vec<Token> {
        vec![Token::ColumnName]
    }
}
