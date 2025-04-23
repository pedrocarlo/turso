//! This file attempts to represent the possible next tokens of each Token in a select query

use std::collections::VecDeque;

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
