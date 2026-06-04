use std::{error::Error, future::Future};

use crate::Result;

/// Event type for state machines that cannot emit events.
///
/// This should be replaced with the never type (`!`) once `!` is stable in
/// associated type positions. See https://github.com/rust-lang/rust/issues/35121.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoEvent {}

/// Synchronous sans-I/O state machine.
///
/// The machine receives one input and produces one finite transition through
/// [`StateMachine::step`]. Callers own the boundary: events are applied outside
/// the machine, and waits are driven by the adapter before the next input.
pub trait StateMachine<Input> {
    type Event;
    type Signal;
    type Output;
    type Error: Error + Send + Sync + 'static;

    fn step(
        &mut self,
        input: Input,
    ) -> Result<Step<Self::Event, Self::Signal, Self::Output>, Self::Error>;

    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// One finite step produced by a [`StateMachine`].
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step<Event, Signal, Output> {
    /// The core has an event for the adapter or application to process.
    Emit(Event),
    /// The core cannot make progress until this signal is satisfied.
    Wait(Signal),
    /// The machine has reached a terminal state and produced its result.
    Done(Output),
}

#[macro_export]
macro_rules! no_event_unreachable {
    ($event:expr) => {{
        let event: $crate::sans_io::NoEvent = $event;
        let _ = event;
        unreachable!("sans-I/O state machine declared no events but emitted one")
    }};
}

#[macro_export]
macro_rules! return_if_sans_io {
    ($expr:expr) => {
        match $expr {
            $crate::sans_io::Step::Done(value) => value,
            $crate::sans_io::Step::Wait(signal) => {
                return Ok($crate::sans_io::Step::Wait(signal));
            }
            $crate::sans_io::Step::Emit(event) => $crate::no_event_unreachable!(event),
        }
    };
}

#[macro_export]
macro_rules! sans_io_yield_one {
    ($signal:expr) => {{
        return Ok($crate::sans_io::Step::Wait($signal));
    }};
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
