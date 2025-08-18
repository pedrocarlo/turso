use std::time::Duration;

use crate::tui::keymap::Cmd;

mod keymap;
mod runner;

pub const TICK_INTERVAL: Duration = Duration::from_millis(30);

pub enum Event {
    Term(Cmd),
    Tick,
    Resize { width: u16, height: u16 },
    Shutdown,
}
