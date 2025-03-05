use std::{rc::Rc, sync::Arc};

use limbo_core::{Connection, StepResult};
use reedline::{Completer, DefaultPrompt, Reedline, Signal, Suggestion};

use crate::readline_utils::{default_break_chars, extract_word, ESCAPE_CHAR};

macro_rules! try_result {
    ($expr:expr, $err:expr) => {
        match $expr {
            Ok(val) => val,
            Err(_) => return $err,
        }
    };
}

pub struct SqlCompleter {
    conn: Rc<Connection>,
    io: Arc<dyn limbo_core::IO>,
}

impl SqlCompleter {
    pub fn new(conn: Rc<Connection>, io: Arc<dyn limbo_core::IO>) -> Self {
        Self { conn, io }
    }
}

impl Completer for SqlCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let _ = (line, pos);
        // TODO: have to differentiate words if they are enclosed in single of double quotes
        let (_, prefix) = extract_word(line, pos, ESCAPE_CHAR, default_break_chars);
        let mut candidates = Vec::new();

        let query = try_result!(
            self.conn.query(format!(
                "SELECT candidate FROM completion('{prefix}', '{line}') ORDER BY 1;"
            )),
            candidates
        );

        if let Some(mut rows) = query {
            loop {
                match try_result!(rows.step(), candidates) {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let completion: &str = try_result!(row.get::<&str>(0), candidates);
                        let candidate = Suggestion {
                            value: completion.to_string(),
                            ..Default::default()
                        };
                        candidates.push(candidate);
                    }
                    StepResult::IO => {
                        try_result!(self.io.run_once(), candidates);
                    }
                    StepResult::Interrupt => break,
                    StepResult::Done => break,
                    StepResult::Busy => {
                        break;
                    }
                }
            }
        }

        candidates
    }

    // Default impl
    fn complete_with_base_ranges(
        &mut self,
        line: &str,
        pos: usize,
    ) -> (Vec<Suggestion>, Vec<std::ops::Range<usize>>) {
        let mut ranges = std::vec![];
        let suggestions = self.complete(line, pos);
        for suggestion in &suggestions {
            ranges.push(suggestion.span.start..suggestion.span.end);
        }
        ranges.dedup();
        (suggestions, ranges)
    }

    // Default impl
    fn partial_complete(
        &mut self,
        line: &str,
        pos: usize,
        start: usize,
        offset: usize,
    ) -> Vec<Suggestion> {
        self.complete(line, pos)
            .into_iter()
            .skip(start)
            .take(offset)
            .collect()
    }

    // Default impl
    fn total_completions(&mut self, line: &str, pos: usize) -> usize {
        self.complete(line, pos).len()
    }
}
