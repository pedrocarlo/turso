use std::{error::Error, future::Future};

use crate::Result;

/// Synchronous sans-I/O state machine.
///
/// The machine receives one input and produces one finite transition through
/// [`StateMachine::step`]. Callers own the boundary: events are applied outside
/// the machine, and waits are driven by the adapter before the next input.
pub trait StateMachine<Input> {
    type Event;
    type Signal;
    type Error: Error + Send + Sync + 'static;

    fn step(&mut self, input: Input) -> Result<Step<Self::Event, Self::Signal>, Self::Error>;

    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// One finite step produced by a [`StateMachine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step<Event, Signal> {
    /// The core has an event for the adapter or application to process.
    Emit(Event),
    /// The core cannot make progress until this signal is satisfied.
    Wait(Signal),
    /// The machine has reached a terminal state.
    Done,
}

/// Async adapter hook for state-machine wait signals.
///
/// Implementations should register listeners synchronously before returning the
/// future so callers can build the wait while holding any transition lock, then
/// await it after releasing that lock.
pub trait StateMachineSignal<I> {
    type Wait<'a>: Future<Output = ()> + 'a
    where
        Self: 'a,
        I: 'a;

    fn wait<'a>(self, io: &'a I) -> Self::Wait<'a>
    where
        Self: 'a;
}
