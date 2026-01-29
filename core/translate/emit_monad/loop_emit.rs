//! # LoopEmit: Iterator-like Combinators for Bytecode Emission Loops
//!
//! This module provides a chumsky-inspired abstraction for handling loop patterns
//! in bytecode emission. Just as chumsky's `IterParser` provides combinators for
//! parsing sequences, `LoopEmit` provides combinators for emitting loops.
//!
//! ## Design Philosophy
//!
//! Loops in bytecode emission follow predictable patterns:
//! ```text
//! setup:
//!     <initialize, e.g., Rewind cursor>
//!     if empty, jump to end
//! loop_start:
//!     <body - emitted for each iteration>
//! next:
//!     <step - e.g., Next instruction>
//!     if more, jump to loop_start
//! end:
//!     <cleanup>
//! ```
//!
//! The `LoopEmit` trait abstracts over this pattern, allowing:
//! - Different loop sources (cursor, sorter, seek-based, etc.)
//! - Combinators for collection, folding, counting, early exit
//! - Composition of nested loops
//!
//! ## Example Usage
//!
//! ```ignore
//! // Simple cursor loop that emits code for each row
//! cursor_loop(cursor_id, |ctx| {
//!     column(ctx.cursor_id, 0, dest_reg)
//! })
//! .emit_all()
//! .run(&mut program)?;
//!
//! // Collect results from each iteration
//! cursor_loop(cursor_id, |ctx| {
//!     column(ctx.cursor_id, 0, temp_reg)
//!         .map(move |_| temp_reg)
//! })
//! .collect()
//! .run(&mut program)?;
//!
//! // Fold with an accumulator
//! cursor_loop(cursor_id, |ctx| {
//!     column(ctx.cursor_id, 0, temp_reg)
//! })
//! .fold_emit(0usize, |acc, _| pure(acc + 1))
//! .run(&mut program)?;
//! ```

#![allow(dead_code)]

use super::*;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::Result;

// =============================================================================
// Core LoopEmit Trait
// =============================================================================

/// A loop structure that emits bytecode for each iteration.
///
/// Analogous to chumsky's `IterParser`, this trait provides combinators for
/// working with loop-based bytecode emission patterns.
///
/// # Type Parameters
/// - `Item`: The type produced by each iteration's body
pub trait LoopEmit: Sized {
    /// The type produced by each iteration's body.
    type Item;

    /// Execute the loop, collecting results into a Vec.
    ///
    /// # Example
    /// ```ignore
    /// cursor_loop(cursor_id, |ctx| {
    ///     column(ctx.cursor_id, 0, reg).map(move |_| reg)
    /// })
    /// .collect()
    /// ```
    #[inline(always)]
    fn collect(self) -> Collect<Self> {
        Collect { loop_emit: self }
    }

    /// Execute the loop, discarding all results.
    ///
    /// Use this when you only care about the side effects (emitted bytecode)
    /// and don't need to track what each iteration produces.
    #[inline(always)]
    fn emit_all(self) -> EmitAll<Self> {
        EmitAll { loop_emit: self }
    }

    /// Execute the loop, counting the number of iterations.
    ///
    /// Note: This counts compile-time iterations (how many times the body
    /// was emitted), not runtime iterations.
    #[inline(always)]
    fn count(self) -> Count<Self> {
        Count { loop_emit: self }
    }

    /// Fold over the loop iterations with an accumulator.
    ///
    /// # Example
    /// ```ignore
    /// cursor_loop(cursor_id, |ctx| {
    ///     column(ctx.cursor_id, 0, temp_reg)
    /// })
    /// .fold_emit(0, |acc, _| pure(acc + 1))
    /// ```
    #[inline(always)]
    fn fold_emit<A, F, E>(self, init: A, f: F) -> FoldEmit<Self, A, F>
    where
        F: FnMut(A, Self::Item) -> E,
        E: Emit<Output = A>,
    {
        FoldEmit {
            loop_emit: self,
            init,
            f,
        }
    }

    /// Transform each iteration's output.
    ///
    /// # Example
    /// ```ignore
    /// cursor_loop(cursor_id, |ctx| {
    ///     column(ctx.cursor_id, 0, reg)
    /// })
    /// .map_item(|_| 42)
    /// .collect()
    /// ```
    #[inline(always)]
    fn map_item<F, B>(self, f: F) -> MapItem<Self, F>
    where
        F: FnMut(Self::Item) -> B,
    {
        MapItem { loop_emit: self, f }
    }

    /// Add an index to each iteration's output.
    ///
    /// # Example
    /// ```ignore
    /// cursor_loop(cursor_id, |ctx| {
    ///     column(ctx.cursor_id, 0, reg)
    /// })
    /// .enumerate()
    /// .collect()  // -> Vec<(usize, ())>
    /// ```
    #[inline(always)]
    fn enumerate(self) -> Enumerate<Self> {
        Enumerate { loop_emit: self }
    }

    /// Filter iterations based on a predicate.
    ///
    /// Only iterations where the predicate returns true will be included.
    #[inline(always)]
    fn filter_item<F>(self, predicate: F) -> FilterItem<Self, F>
    where
        F: FnMut(&Self::Item) -> bool,
    {
        FilterItem {
            loop_emit: self,
            predicate,
        }
    }

    /// Take only the first `n` iterations.
    ///
    /// Note: This affects compile-time emission, not runtime behavior.
    /// For runtime LIMIT, use the `with_limit` combinator instead.
    #[inline(always)]
    fn take(self, n: usize) -> Take<Self> {
        Take { loop_emit: self, n }
    }

    /// Skip the first `n` iterations.
    #[inline(always)]
    fn skip(self, n: usize) -> Skip<Self> {
        Skip { loop_emit: self, n }
    }

    /// Chain two loops together.
    ///
    /// The second loop's iterations follow the first loop's iterations.
    #[inline(always)]
    fn chain<L2>(self, other: L2) -> Chain<Self, L2>
    where
        L2: LoopEmit<Item = Self::Item>,
    {
        Chain {
            first: self,
            second: other,
        }
    }

    /// Add a runtime limit check to the loop.
    ///
    /// This emits bytecode that checks a counter against a limit register
    /// and exits the loop early if the limit is reached.
    #[inline(always)]
    fn with_limit(self, limit_reg: usize, counter_reg: usize) -> WithLimit<Self> {
        WithLimit {
            loop_emit: self,
            limit_reg,
            counter_reg,
        }
    }

    /// Add a runtime offset (skip) to the loop.
    ///
    /// This emits bytecode that skips the first N rows at runtime.
    #[inline(always)]
    fn with_offset(self, offset_reg: usize, counter_reg: usize) -> WithOffset<Self> {
        WithOffset {
            loop_emit: self,
            offset_reg,
            counter_reg,
        }
    }

    /// Execute this loop, then run another emit computation.
    #[inline(always)]
    fn then_emit<F, E>(self, f: F) -> ThenEmit<Self, F>
    where
        F: FnOnce(Vec<Self::Item>) -> E,
        E: Emit,
    {
        ThenEmit { loop_emit: self, f }
    }

    /// Internal method to run the loop with a visitor callback.
    ///
    /// Implementations should call `visitor` for each iteration's result.
    fn run_with_visitor<V>(self, program: &mut ProgramBuilder, visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>;
}

// =============================================================================
// Loop Context
// =============================================================================

/// Context provided to loop bodies.
///
/// Contains information about the current loop structure that the body
/// may need to reference (e.g., for early exit jumps).
#[derive(Clone, Copy)]
pub struct LoopContext {
    /// The cursor being iterated (if applicable).
    pub cursor_id: usize,
    /// Labels for loop control flow.
    pub labels: LoopLabels,
}

// =============================================================================
// Cursor Loop
// =============================================================================

/// A loop over a cursor using Rewind/Next pattern.
///
/// This is the most common loop pattern in SQLite bytecode.
pub struct CursorLoop<F> {
    cursor_id: usize,
    body: F,
}

/// Create a cursor-based loop (Rewind/Next pattern).
///
/// # Example
/// ```ignore
/// cursor_loop(cursor_id, |ctx| {
///     column(ctx.cursor_id, 0, dest_reg)
/// })
/// .emit_all()
/// ```
#[inline(always)]
pub fn cursor_loop<F, E>(cursor_id: usize, body: F) -> CursorLoop<F>
where
    F: FnMut(LoopContext) -> E,
    E: Emit,
{
    CursorLoop { cursor_id, body }
}

impl<F, E> LoopEmit for CursorLoop<F>
where
    F: FnMut(LoopContext) -> E,
    E: Emit,
{
    type Item = E::Output;

    fn run_with_visitor<V>(mut self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        let start_label = program.allocate_label();
        let end_label = program.allocate_label();
        let next_label = program.allocate_label();

        let labels = LoopLabels {
            start: start_label,
            end: end_label,
            next: next_label,
        };

        let ctx = LoopContext {
            cursor_id: self.cursor_id,
            labels,
        };

        // Emit Rewind
        program.emit_insn(Insn::Rewind {
            cursor_id: self.cursor_id,
            pc_if_empty: end_label,
        });

        // Mark loop start
        program.preassign_label_to_next_insn(start_label);

        // Run body
        let result = (self.body)(ctx).run(program)?;
        visitor(result)?;

        // Mark next label
        program.resolve_label(next_label, program.offset());

        // Emit Next
        program.emit_insn(Insn::Next {
            cursor_id: self.cursor_id,
            pc_if_next: start_label,
        });

        // Mark end label
        program.preassign_label_to_next_insn(end_label);

        Ok(())
    }
}

// =============================================================================
// Sorter Loop
// =============================================================================

/// A loop over a sorter using SorterSort/SorterNext pattern.
pub struct SorterLoop<F> {
    cursor_id: usize,
    body: F,
}

/// Create a sorter-based loop (SorterSort/SorterNext pattern).
///
/// # Example
/// ```ignore
/// sorter_loop(sorter_cursor, |ctx| {
///     sorter_data(ctx.cursor_id, dest_reg)
/// })
/// .emit_all()
/// ```
#[inline(always)]
pub fn sorter_loop<F, E>(cursor_id: usize, body: F) -> SorterLoop<F>
where
    F: FnMut(LoopContext) -> E,
    E: Emit,
{
    SorterLoop { cursor_id, body }
}

impl<F, E> LoopEmit for SorterLoop<F>
where
    F: FnMut(LoopContext) -> E,
    E: Emit,
{
    type Item = E::Output;

    fn run_with_visitor<V>(mut self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        let start_label = program.allocate_label();
        let end_label = program.allocate_label();
        let next_label = program.allocate_label();

        let labels = LoopLabels {
            start: start_label,
            end: end_label,
            next: next_label,
        };

        let ctx = LoopContext {
            cursor_id: self.cursor_id,
            labels,
        };

        // Emit SorterSort
        program.emit_insn(Insn::SorterSort {
            cursor_id: self.cursor_id,
            pc_if_empty: end_label,
        });

        // Mark loop start
        program.preassign_label_to_next_insn(start_label);

        // Run body
        let result = (self.body)(ctx).run(program)?;
        visitor(result)?;

        // Mark next label
        program.resolve_label(next_label, program.offset());

        // Emit SorterNext
        program.emit_insn(Insn::SorterNext {
            cursor_id: self.cursor_id,
            pc_if_next: start_label,
        });

        // Mark end label
        program.preassign_label_to_next_insn(end_label);

        Ok(())
    }
}

// =============================================================================
// Backwards Cursor Loop (Last/Prev pattern)
// =============================================================================

/// A loop over a cursor in reverse using Last/Prev pattern.
pub struct ReverseCursorLoop<F> {
    cursor_id: usize,
    body: F,
}

/// Create a reverse cursor loop (Last/Prev pattern).
///
/// # Example
/// ```ignore
/// reverse_cursor_loop(cursor_id, |ctx| {
///     column(ctx.cursor_id, 0, dest_reg)
/// })
/// .emit_all()
/// ```
#[inline(always)]
pub fn reverse_cursor_loop<F, E>(cursor_id: usize, body: F) -> ReverseCursorLoop<F>
where
    F: FnMut(LoopContext) -> E,
    E: Emit,
{
    ReverseCursorLoop { cursor_id, body }
}

impl<F, E> LoopEmit for ReverseCursorLoop<F>
where
    F: FnMut(LoopContext) -> E,
    E: Emit,
{
    type Item = E::Output;

    fn run_with_visitor<V>(mut self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        let start_label = program.allocate_label();
        let end_label = program.allocate_label();
        let next_label = program.allocate_label();

        let labels = LoopLabels {
            start: start_label,
            end: end_label,
            next: next_label,
        };

        let ctx = LoopContext {
            cursor_id: self.cursor_id,
            labels,
        };

        // Emit Last
        program.emit_insn(Insn::Last {
            cursor_id: self.cursor_id,
            pc_if_empty: end_label,
        });

        // Mark loop start
        program.preassign_label_to_next_insn(start_label);

        // Run body
        let result = (self.body)(ctx).run(program)?;
        visitor(result)?;

        // Mark next label
        program.resolve_label(next_label, program.offset());

        // Emit Prev
        program.emit_insn(Insn::Prev {
            cursor_id: self.cursor_id,
            pc_if_prev: start_label,
        });

        // Mark end label
        program.preassign_label_to_next_insn(end_label);

        Ok(())
    }
}

// =============================================================================
// Generic Loop (for custom loop patterns)
// =============================================================================

/// A generic loop with customizable setup, step, and cleanup.
///
/// Use this when the standard loop patterns don't fit your needs.
pub struct GenericLoop<Setup, Body, Step, Cleanup> {
    setup: Setup,
    body: Body,
    step: Step,
    cleanup: Cleanup,
}

/// Create a generic loop with custom setup, body, step, and cleanup.
///
/// # Example
/// ```ignore
/// generic_loop(
///     |labels| rewind(cursor_id, labels.end),
///     |labels| column(cursor_id, 0, dest_reg),
///     |labels| next(cursor_id, labels.start),
///     |labels| pure(()),
/// )
/// .emit_all()
/// ```
#[inline(always)]
pub fn generic_loop<Setup, Body, Step, Cleanup>(
    setup: Setup,
    body: Body,
    step: Step,
    cleanup: Cleanup,
) -> GenericLoop<Setup, Body, Step, Cleanup> {
    GenericLoop {
        setup,
        body,
        step,
        cleanup,
    }
}

impl<Setup, Body, Step, Cleanup, SetupE, BodyE, StepE, CleanupE> LoopEmit
    for GenericLoop<Setup, Body, Step, Cleanup>
where
    Setup: FnOnce(LoopLabels) -> SetupE,
    Body: FnMut(LoopLabels) -> BodyE,
    Step: FnOnce(LoopLabels) -> StepE,
    Cleanup: FnOnce(LoopLabels) -> CleanupE,
    SetupE: Emit,
    BodyE: Emit,
    StepE: Emit,
    CleanupE: Emit,
{
    type Item = BodyE::Output;

    fn run_with_visitor<V>(mut self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        let start_label = program.allocate_label();
        let end_label = program.allocate_label();
        let next_label = program.allocate_label();

        let labels = LoopLabels {
            start: start_label,
            end: end_label,
            next: next_label,
        };

        // Run setup
        (self.setup)(labels).run(program)?;

        // Mark loop start
        program.preassign_label_to_next_insn(start_label);

        // Run body
        let result = (self.body)(labels).run(program)?;
        visitor(result)?;

        // Mark next label
        program.resolve_label(next_label, program.offset());

        // Run step
        (self.step)(labels).run(program)?;

        // Mark end label
        program.preassign_label_to_next_insn(end_label);

        // Run cleanup
        (self.cleanup)(labels).run(program)?;

        Ok(())
    }
}

// =============================================================================
// Static Iterator Loop (for compile-time known iterations)
// =============================================================================

/// A loop over a static iterator (compile-time known iterations).
///
/// Unlike cursor loops which emit runtime iteration code, this loop
/// emits the body once for each item in the iterator at compile time.
pub struct StaticIter<I, F> {
    iter: I,
    body: F,
}

/// Create a loop over a static iterator.
///
/// This emits the body once for each item in the iterator at compile time.
/// Useful for emitting code for each column, each table, etc.
///
/// # Example
/// ```ignore
/// static_iter(0..num_columns, |col_idx| {
///     column(cursor_id, col_idx, base_reg + col_idx)
/// })
/// .emit_all()
/// ```
#[inline(always)]
pub fn static_iter<I, F, E>(iter: I, body: F) -> StaticIter<I, F>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> E,
    E: Emit,
{
    StaticIter { iter, body }
}

impl<I, F, E> LoopEmit for StaticIter<I, F>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> E,
    E: Emit,
{
    type Item = E::Output;

    fn run_with_visitor<V>(mut self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        for item in self.iter {
            let result = (self.body)(item).run(program)?;
            visitor(result)?;
        }
        Ok(())
    }
}

// =============================================================================
// Combinator Implementations
// =============================================================================

/// Result of `.collect()` on a LoopEmit.
pub struct Collect<L> {
    loop_emit: L,
}

impl<L: LoopEmit> Emit for Collect<L> {
    type Output = Vec<L::Item>;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let mut results = Vec::new();
        self.loop_emit.run_with_visitor(program, |item| {
            results.push(item);
            Ok(())
        })?;
        Ok(results)
    }
}

/// Result of `.emit_all()` on a LoopEmit.
pub struct EmitAll<L> {
    loop_emit: L,
}

impl<L: LoopEmit> Emit for EmitAll<L> {
    type Output = ();

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        self.loop_emit.run_with_visitor(program, |_| Ok(()))
    }
}

/// Result of `.count()` on a LoopEmit.
pub struct Count<L> {
    loop_emit: L,
}

impl<L: LoopEmit> Emit for Count<L> {
    type Output = usize;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let mut count = 0;
        self.loop_emit.run_with_visitor(program, |_| {
            count += 1;
            Ok(())
        })?;
        Ok(count)
    }
}

/// Result of `.fold_emit()` on a LoopEmit.
pub struct FoldEmit<L, A, F> {
    loop_emit: L,
    init: A,
    f: F,
}

impl<L, A, F, E> Emit for FoldEmit<L, A, F>
where
    L: LoopEmit,
    F: FnMut(A, L::Item) -> E,
    E: Emit<Output = A>,
{
    type Output = A;

    #[inline(always)]
    fn run(mut self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let mut acc = self.init;
        // Note: We need to collect first since we can't have mutable borrows of both
        // program and self.f at the same time in the visitor closure.
        let items = self.loop_emit.collect().run(program)?;
        for item in items {
            acc = (self.f)(acc, item).run(program)?;
        }
        Ok(acc)
    }
}

/// Result of `.map_item()` on a LoopEmit.
pub struct MapItem<L, F> {
    loop_emit: L,
    f: F,
}

impl<L, F, B> LoopEmit for MapItem<L, F>
where
    L: LoopEmit,
    F: FnMut(L::Item) -> B,
{
    type Item = B;

    fn run_with_visitor<V>(mut self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        self.loop_emit
            .run_with_visitor(program, |item| visitor((self.f)(item)))
    }
}

/// Result of `.enumerate()` on a LoopEmit.
pub struct Enumerate<L> {
    loop_emit: L,
}

impl<L: LoopEmit> LoopEmit for Enumerate<L> {
    type Item = (usize, L::Item);

    fn run_with_visitor<V>(self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        let mut index = 0;
        self.loop_emit.run_with_visitor(program, |item| {
            let result = visitor((index, item));
            index += 1;
            result
        })
    }
}

/// Result of `.filter_item()` on a LoopEmit.
pub struct FilterItem<L, F> {
    loop_emit: L,
    predicate: F,
}

impl<L, F> LoopEmit for FilterItem<L, F>
where
    L: LoopEmit,
    F: FnMut(&L::Item) -> bool,
{
    type Item = L::Item;

    fn run_with_visitor<V>(mut self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        self.loop_emit.run_with_visitor(program, |item| {
            if (self.predicate)(&item) {
                visitor(item)
            } else {
                Ok(())
            }
        })
    }
}

/// Result of `.take()` on a LoopEmit.
pub struct Take<L> {
    loop_emit: L,
    n: usize,
}

impl<L: LoopEmit> LoopEmit for Take<L> {
    type Item = L::Item;

    fn run_with_visitor<V>(self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        let mut count = 0;
        let n = self.n;
        self.loop_emit.run_with_visitor(program, |item| {
            if count < n {
                count += 1;
                visitor(item)
            } else {
                Ok(())
            }
        })
    }
}

/// Result of `.skip()` on a LoopEmit.
pub struct Skip<L> {
    loop_emit: L,
    n: usize,
}

impl<L: LoopEmit> LoopEmit for Skip<L> {
    type Item = L::Item;

    fn run_with_visitor<V>(self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        let mut count = 0;
        let n = self.n;
        self.loop_emit.run_with_visitor(program, |item| {
            if count >= n {
                visitor(item)
            } else {
                count += 1;
                Ok(())
            }
        })
    }
}

/// Result of `.chain()` on a LoopEmit.
pub struct Chain<L1, L2> {
    first: L1,
    second: L2,
}

impl<L1, L2> LoopEmit for Chain<L1, L2>
where
    L1: LoopEmit,
    L2: LoopEmit<Item = L1::Item>,
{
    type Item = L1::Item;

    fn run_with_visitor<V>(self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        self.first.run_with_visitor(program, &mut visitor)?;
        self.second.run_with_visitor(program, visitor)
    }
}

/// Result of `.with_limit()` on a LoopEmit.
///
/// Note: This is a marker for compile-time limit tracking. For runtime
/// limit checks, emit the limit logic in your loop body.
pub struct WithLimit<L> {
    loop_emit: L,
    limit_reg: usize,
    counter_reg: usize,
}

impl<L: LoopEmit> LoopEmit for WithLimit<L> {
    type Item = (L::Item, usize, usize); // (item, limit_reg, counter_reg)

    fn run_with_visitor<V>(self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        let limit_reg = self.limit_reg;
        let counter_reg = self.counter_reg;
        self.loop_emit
            .run_with_visitor(program, |item| visitor((item, limit_reg, counter_reg)))
    }
}

/// Result of `.with_offset()` on a LoopEmit.
///
/// Note: This is a marker for compile-time offset tracking. For runtime
/// offset checks, emit the offset logic in your loop body.
pub struct WithOffset<L> {
    loop_emit: L,
    offset_reg: usize,
    counter_reg: usize,
}

impl<L: LoopEmit> LoopEmit for WithOffset<L> {
    type Item = (L::Item, usize, usize); // (item, offset_reg, counter_reg)

    fn run_with_visitor<V>(self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        let offset_reg = self.offset_reg;
        let counter_reg = self.counter_reg;
        self.loop_emit
            .run_with_visitor(program, |item| visitor((item, offset_reg, counter_reg)))
    }
}

/// Result of `.then_emit()` on a LoopEmit.
pub struct ThenEmit<L, F> {
    loop_emit: L,
    f: F,
}

impl<L, F, E> Emit for ThenEmit<L, F>
where
    L: LoopEmit,
    F: FnOnce(Vec<L::Item>) -> E,
    E: Emit,
{
    type Output = E::Output;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let items = self.loop_emit.collect().run(program)?;
        (self.f)(items).run(program)
    }
}

// =============================================================================
// Nested Loop Support
// =============================================================================

/// A nested loop structure.
///
/// Represents an outer loop where each iteration spawns an inner loop.
pub struct NestedLoop<Outer, Inner> {
    outer: Outer,
    inner: Inner,
}

/// Create a nested loop structure.
///
/// # Example
/// ```ignore
/// nested_loop(
///     cursor_loop(outer_cursor, |ctx| pure(ctx.cursor_id)),
///     |outer_cursor_id| cursor_loop(inner_cursor, |ctx| {
///         column(ctx.cursor_id, 0, dest_reg)
///     }),
/// )
/// .emit_all()
/// ```
#[inline(always)]
pub fn nested_loop<Outer, Inner, InnerLoop>(outer: Outer, inner: Inner) -> NestedLoop<Outer, Inner>
where
    Outer: LoopEmit,
    Inner: FnMut(Outer::Item) -> InnerLoop,
    InnerLoop: LoopEmit,
{
    NestedLoop { outer, inner }
}

impl<Outer, Inner, InnerLoop> LoopEmit for NestedLoop<Outer, Inner>
where
    Outer: LoopEmit,
    Inner: FnMut(Outer::Item) -> InnerLoop,
    InnerLoop: LoopEmit,
{
    type Item = InnerLoop::Item;

    fn run_with_visitor<V>(mut self, program: &mut ProgramBuilder, mut visitor: V) -> Result<()>
    where
        V: FnMut(Self::Item) -> Result<()>,
    {
        // For nested loops, we need to collect outer results first to avoid
        // borrow conflicts, then iterate over them.
        let outer_items = self.outer.collect().run(program)?;
        for outer_item in outer_items {
            let inner_loop = (self.inner)(outer_item);
            inner_loop.run_with_visitor(program, &mut visitor)?;
        }
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};
    use crate::CaptureDataChangesMode;

    fn test_program() -> ProgramBuilder {
        ProgramBuilder::new(
            QueryMode::Normal,
            CaptureDataChangesMode::Off,
            ProgramBuilderOpts {
                num_cursors: 5,
                approx_num_insns: 50,
                approx_num_labels: 20,
            },
        )
    }

    #[test]
    fn test_static_iter_collect() {
        let mut program = test_program();

        let result = static_iter(0..3, |i| pure(i * 2))
            .collect()
            .run(&mut program)
            .unwrap();

        assert_eq!(result, vec![0, 2, 4]);
    }

    #[test]
    fn test_static_iter_emit_all() {
        let mut program = test_program();

        static_iter(0..3, |i| integer(i as i64, 1))
            .emit_all()
            .run(&mut program)
            .unwrap();

        // Should have emitted 3 integer instructions
        assert_eq!(program.insns.len(), 3);
    }

    #[test]
    fn test_static_iter_map_item() {
        let mut program = test_program();

        let result = static_iter(0..3, |i| pure(i))
            .map_item(|x| x * 10)
            .collect()
            .run(&mut program)
            .unwrap();

        assert_eq!(result, vec![0, 10, 20]);
    }

    #[test]
    fn test_static_iter_enumerate() {
        let mut program = test_program();

        let result = static_iter(vec!["a", "b", "c"].into_iter(), |s| pure(s))
            .enumerate()
            .collect()
            .run(&mut program)
            .unwrap();

        assert_eq!(result, vec![(0, "a"), (1, "b"), (2, "c")]);
    }

    #[test]
    fn test_static_iter_filter() {
        let mut program = test_program();

        let result = static_iter(0..5, |i| pure(i))
            .filter_item(|&x| x % 2 == 0)
            .collect()
            .run(&mut program)
            .unwrap();

        assert_eq!(result, vec![0, 2, 4]);
    }

    #[test]
    fn test_static_iter_take() {
        let mut program = test_program();

        let result = static_iter(0..10, |i| pure(i))
            .take(3)
            .collect()
            .run(&mut program)
            .unwrap();

        assert_eq!(result, vec![0, 1, 2]);
    }

    #[test]
    fn test_static_iter_skip() {
        let mut program = test_program();

        let result = static_iter(0..5, |i| pure(i))
            .skip(2)
            .collect()
            .run(&mut program)
            .unwrap();

        assert_eq!(result, vec![2, 3, 4]);
    }

    #[test]
    fn test_static_iter_chain() {
        let mut program = test_program();

        let result = static_iter(0..2, |i| pure(i))
            .chain(static_iter(10..12, |i| pure(i)))
            .collect()
            .run(&mut program)
            .unwrap();

        assert_eq!(result, vec![0, 1, 10, 11]);
    }

    #[test]
    fn test_static_iter_count() {
        let mut program = test_program();

        let result = static_iter(0..5, |i| pure(i))
            .count()
            .run(&mut program)
            .unwrap();

        assert_eq!(result, 5);
    }

    #[test]
    fn test_cursor_loop_structure() {
        let mut program = test_program();

        cursor_loop(0, |ctx| {
            // Just emit a column read
            insn(Insn::Column {
                cursor_id: ctx.cursor_id,
                column: 0,
                dest: 1,
                default: None,
            })
        })
        .emit_all()
        .run(&mut program)
        .unwrap();

        // Should have: Rewind, Column, Next
        assert_eq!(program.insns.len(), 3);

        // Verify instruction types (insns is Vec<(Insn, usize)>)
        assert!(matches!(program.insns[0].0, Insn::Rewind { .. }));
        assert!(matches!(program.insns[1].0, Insn::Column { .. }));
        assert!(matches!(program.insns[2].0, Insn::Next { .. }));
    }

    #[test]
    fn test_sorter_loop_structure() {
        let mut program = test_program();

        sorter_loop(0, |_ctx| pure(()))
            .emit_all()
            .run(&mut program)
            .unwrap();

        // Should have: SorterSort, SorterNext
        assert_eq!(program.insns.len(), 2);

        assert!(matches!(program.insns[0].0, Insn::SorterSort { .. }));
        assert!(matches!(program.insns[1].0, Insn::SorterNext { .. }));
    }

    #[test]
    fn test_generic_loop() {
        let mut program = test_program();

        generic_loop(
            |labels: LoopLabels| {
                insn(Insn::Rewind {
                    cursor_id: 0,
                    pc_if_empty: labels.end,
                })
            },
            |_labels: LoopLabels| pure(42),
            |labels: LoopLabels| {
                insn(Insn::Next {
                    cursor_id: 0,
                    pc_if_next: labels.start,
                })
            },
            |_labels: LoopLabels| pure(()),
        )
        .emit_all()
        .run(&mut program)
        .unwrap();

        assert_eq!(program.insns.len(), 2);
    }

    #[test]
    fn test_fold_emit() {
        let mut program = test_program();

        let result = static_iter(1..=4, |i| pure(i))
            .fold_emit(0, |acc, x| pure(acc + x))
            .run(&mut program)
            .unwrap();

        assert_eq!(result, 10); // 1 + 2 + 3 + 4
    }

    #[test]
    fn test_then_emit() {
        let mut program = test_program();

        let result = static_iter(0..3, |i| pure(i))
            .then_emit(|items| pure(items.len()))
            .run(&mut program)
            .unwrap();

        assert_eq!(result, 3);
    }
}
