use crossterm::event::{KeyCode, KeyEvent};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cmd {
    Enter,

    Exit,

    Toggle,

    Up,

    Down,

    Right,

    Left,

    Yes,

    No,

    Tick,
}

/// Return the [`Cmd`] that gets interpreted or `None` if the key press is part
/// of a sequence or not a valid key press.
#[derive(Debug, Default, Clone, Copy)]
pub struct KeyHandler;

impl KeyHandler {
    pub fn on(&mut self, event: KeyEvent) -> Option<Cmd> {
        let cmd = match event.code {
            KeyCode::Enter => Cmd::Enter,
            KeyCode::Esc => Cmd::Exit,
            KeyCode::Char(' ') => Cmd::Toggle,
            KeyCode::Up => Cmd::Up,
            KeyCode::Down => Cmd::Down,
            KeyCode::Right => Cmd::Right,
            KeyCode::Left => Cmd::Left,
            KeyCode::Char('y') | KeyCode::Char('Y') => Cmd::Yes,
            KeyCode::Char('n') | KeyCode::Char('N') => Cmd::No,
            _ => return None,
        };
        Some(cmd)
    }
}
