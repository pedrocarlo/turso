use std::io::{stdout, Stdout};

use crossterm::event::EventStream;
use crossterm::{
    event::{Event as TermEvent, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use ratatui::prelude::*;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::interval,
};

use crate::tui::{keymap::KeyHandler, Event, TICK_INTERVAL};

pub type Term = Terminal<CrosstermBackend<Stdout>>;

pub struct RunnerCore {
    pub terminal: Term,
}

impl RunnerCore {
    pub fn new() -> Self {
        Self {
            terminal: Terminal::new(CrosstermBackend::new(stdout())).unwrap(),
        }
    }

    /// Handle an individual [Event]
    ///
    /// Return true on [Event::Shutdown], false otherwise.
    pub fn handle_event(&mut self, event: Event) -> anyhow::Result<bool> {
        match event {
            Event::Term(..) => {}
            Event::Tick => {}
            Event::Resize { .. } => {}
            Event::Shutdown => return Ok(true),
        }
        Ok(false)
    }
}

pub struct Runner {
    core: RunnerCore,

    /// The [Runner]'s main_loop is purely single threaded. Every interaction
    /// with the outside world is via channels. All input from the outside world
    /// comes in via an `Event` over a single channel.
    events_rx: UnboundedReceiver<Event>,

    /// We save a copy here so we can hand it out to event producers
    events_tx: UnboundedSender<Event>,

    /// The tokio runtime for everything outside the main thread
    tokio_rt: tokio::runtime::Runtime,
}

impl Runner {
    pub fn new() -> Runner {
        let (events_tx, events_rx) = unbounded_channel();
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let core = RunnerCore::new();
        Runner {
            core,
            events_rx,
            events_tx,
            tokio_rt,
        }
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        self.start_tokio_runtime();
        enable_raw_mode()?;
        execute!(self.core.terminal.backend_mut(), EnterAlternateScreen)?;
        self.main_loop()?;
        disable_raw_mode()?;
        execute!(self.core.terminal.backend_mut(), LeaveAlternateScreen)?;
        Ok(())
    }

    fn main_loop(&mut self) -> anyhow::Result<()> {
        tracing::info!("Starting main loop");

        loop {
            // unwrap is safe because we always hold onto a UnboundedSender
            let event = self.events_rx.blocking_recv().unwrap();
            if self.core.handle_event(event)? {
                // Event::Shutdown received
                break;
            }
        }
        Ok(())
    }

    fn start_tokio_runtime(&mut self) {
        let events_tx = self.events_tx.clone();
        self.tokio_rt.block_on(async {
            run_event_listener(events_tx).await;
        });
    }
}

fn is_control_c(key_event: &KeyEvent) -> bool {
    key_event.code == KeyCode::Char('c') && key_event.modifiers == KeyModifiers::CONTROL
}

/// Listen for terminal related events
async fn run_event_listener(events_tx: UnboundedSender<Event>) {
    tracing::info!("Starting event listener");
    tokio::spawn(async move {
        let mut events = EventStream::new();
        let mut ticker = interval(TICK_INTERVAL);
        let mut key_handler = KeyHandler;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if events_tx.send(Event::Tick).is_err() {
                        tracing::info!("Event listener completed");
                        // The receiver was dropped. Program is ending.
                        return;
                    }
                }
                event = events.next() => {
                    let event = match event {
                        None => {
                            tracing::error!("Event stream completed. Shutting down.");
                            return;
                        }
                        Some(Ok(event)) => event,
                        Some(Err(e)) => {
                            if events_tx.send(Event::Shutdown).is_err() {
                                tracing::info!("Event listener completed");
                                return;
                            }
                            tracing::error!("Failed to receive event: {:?}", e);
                            return;
                        }
                    };

                    let event = match event {
                        TermEvent::Key(key_event) => {
                            if is_control_c(&key_event) {
                                tracing::info!("CTRL-C Pressed. Exiting.");
                                Some(Event::Shutdown)
                            } else {
                                key_handler.on(key_event).map(Event::Term)
                            }
                        }
                        TermEvent::Resize(width, height) => {
                            Some(Event::Resize{width, height})
                        }
                         _ => None
                    };

                    if let Some(event) = event {
                        if events_tx.send(event).is_err() {
                            tracing::info!("Event listener completed");
                            return;
                        }
                    }
                }
            }
        }
    });
}
