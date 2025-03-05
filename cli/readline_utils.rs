// Got this function from rustyline

/// Given a `line` and a cursor `pos`ition,
/// try to find backward the start of a word.
///
/// Return (0, `line[..pos]`) if no break char has been found.
/// Return the word and its start position (idx, `line[idx..pos]`) otherwise.
pub fn extract_word(
    line: &str,
    pos: usize,
    esc_char: Option<char>,
    is_break_char: fn(char) -> bool,
) -> (usize, &str) {
    let line = &line[..pos];
    if line.is_empty() {
        return (0, line);
    }
    let mut start = None;
    for (i, c) in line.char_indices().rev() {
        if let (Some(esc_char), true) = (esc_char, start.is_some()) {
            if esc_char == c {
                // escaped break char
                start = None;
                continue;
            }
            break;
        }
        if is_break_char(c) {
            start = Some(i + c.len_utf8());
            if esc_char.is_none() {
                break;
            } // else maybe escaped...
        }
    }

    match start {
        Some(start) => (start, &line[start..]),
        None => (0, line),
    }
}

// Got this from the FilenameCompleter.
// TODO have to see what chars break words in Sqlite
cfg_if::cfg_if! {
    if #[cfg(unix)] {
        // rl_basic_word_break_characters, rl_completer_word_break_characters
        pub const fn default_break_chars(c : char) -> bool {
            matches!(c, ' ' | '\t' | '\n' | '"' | '\\' | '\'' | '`' | '@' | '$' | '>' | '<' | '=' | ';' | '|' | '&' |
            '{' | '(' | '\0')
        }
        pub const ESCAPE_CHAR: Option<char> = Some('\\');
        // In double quotes, not all break_chars need to be escaped
        // https://www.gnu.org/software/bash/manual/html_node/Double-Quotes.html
        #[allow(dead_code)]
        pub const fn double_quotes_special_chars(c: char) -> bool { matches!(c, '"' | '$' | '\\' | '`') }
    } else if #[cfg(windows)] {
        // Remove \ to make file completion works on windows
        pub const fn default_break_chars(c: char) -> bool {
            matches!(c, ' ' | '\t' | '\n' | '"' | '\'' | '`' | '@' | '$' | '>' | '<' | '=' | ';' | '|' | '&' | '{' |
            '(' | '\0')
        }
        pub const ESCAPE_CHAR: Option<char> = None;
        #[allow(dead_code)]
        pub const fn double_quotes_special_chars(c: char) -> bool { c == '"' } // TODO Validate: only '"' ?
    } else if #[cfg(target_arch = "wasm32")] {
        pub const fn default_break_chars(c: char) -> bool { false }
        pub const ESCAPE_CHAR: Option<char> = None;
        #[allow(dead_code)]
        pub const fn double_quotes_special_chars(c: char) -> bool { false }
    }
}