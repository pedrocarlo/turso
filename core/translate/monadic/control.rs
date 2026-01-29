//! Control flow combinators for the monadic emitter.
//!
//! This module provides high-level combinators for common bytecode patterns:
//! - Loops (forward, backward, nested)
//! - Conditionals (if/then/else in bytecode)
//! - Subroutines and coroutines

// Note: This module provides a public API for control flow patterns.
// Many functions may not be used internally yet but are provided for consumers.
#![allow(dead_code)]

use super::alloc::{alloc_label, alloc_loop_labels, alloc_reg, bind_label, emit};
use super::insn::InsnSpec;
use super::types::{Cursor, Emit, Label, LoopLabels, Reg};

// =============================================================================
// Loop Context
// =============================================================================

/// Context available within a loop body.
///
/// Provides access to the loop's cursor and labels for early exit
/// or continuing to the next iteration.
#[derive(Debug, Clone, Copy)]
pub struct LoopContext {
    /// The cursor being iterated.
    pub cursor: Cursor,
    /// Labels for loop control.
    pub labels: LoopLabels,
}

impl LoopContext {
    /// Create a new loop context.
    pub fn new(cursor: Cursor, labels: LoopLabels) -> Self {
        Self { cursor, labels }
    }

    /// Get the label to jump to for early loop exit.
    pub fn break_label(&self) -> Label {
        self.labels.end
    }

    /// Get the label to jump to for continuing to next iteration.
    pub fn continue_label(&self) -> Label {
        self.labels.next
    }
}

// =============================================================================
// Forward Loops
// =============================================================================

/// Emit a forward loop over a cursor (Rewind...Next).
///
/// This is the most common loop pattern, iterating from first to last row.
///
/// # Example
///
/// ```ignore
/// for_each(cursor, |ctx| {
///     // Read column and emit result
///     read_column(ctx.cursor, 0).flat_map(|val| {
///         emit_result_row(val, 1)
///     })
/// })
/// ```
pub fn for_each<'a, F>(cursor: Cursor, body: F) -> Emit<'a, ()>
where
    F: FnOnce(LoopContext) -> Emit<'a, ()> + 'a,
{
    alloc_loop_labels().flat_map(move |labels| {
        let ctx = LoopContext::new(cursor, labels);

        // Rewind cursor, jump to end if empty
        emit(InsnSpec::Rewind {
            cursor,
            if_empty: labels.end,
        })
        // Bind loop start label
        .then(bind_label(labels.start))
        // Execute body
        .then(body(ctx))
        // Bind next label
        .then(bind_label(labels.next))
        // Next iteration or exit
        .then(emit(InsnSpec::Next {
            cursor,
            if_next: labels.start,
        }))
        // Bind end label
        .then(bind_label(labels.end))
    })
}

/// Emit a forward loop with access to the loop labels.
///
/// Like `for_each`, but returns the loop labels for external control.
pub fn for_each_with_labels<'a, F>(cursor: Cursor, body: F) -> Emit<'a, LoopLabels>
where
    F: FnOnce(LoopContext) -> Emit<'a, ()> + 'a,
{
    alloc_loop_labels().flat_map(move |labels| {
        let ctx = LoopContext::new(cursor, labels);

        emit(InsnSpec::Rewind {
            cursor,
            if_empty: labels.end,
        })
        .then(bind_label(labels.start))
        .then(body(ctx))
        .then(bind_label(labels.next))
        .then(emit(InsnSpec::Next {
            cursor,
            if_next: labels.start,
        }))
        .then(bind_label(labels.end))
        .map(move |_| labels)
    })
}

// =============================================================================
// Backward Loops
// =============================================================================

/// Emit a backward loop over a cursor (Last...Prev).
///
/// Iterates from last to first row.
pub fn for_each_rev<'a, F>(cursor: Cursor, body: F) -> Emit<'a, ()>
where
    F: FnOnce(LoopContext) -> Emit<'a, ()> + 'a,
{
    alloc_loop_labels().flat_map(move |labels| {
        let ctx = LoopContext::new(cursor, labels);

        emit(InsnSpec::Last {
            cursor,
            if_empty: labels.end,
        })
        .then(bind_label(labels.start))
        .then(body(ctx))
        .then(bind_label(labels.next))
        .then(emit(InsnSpec::Prev {
            cursor,
            if_prev: labels.start,
        }))
        .then(bind_label(labels.end))
    })
}

// =============================================================================
// Nested Loops
// =============================================================================

/// Emit nested loops for joins.
///
/// Creates an outer loop and an inner loop, with the body receiving
/// contexts for both loops.
///
/// # Example
///
/// ```ignore
/// nested_loop(users_cursor, orders_cursor, |outer, inner| {
///     // Emit joined columns from both tables
/// })
/// ```
pub fn nested_loop<'a, F>(outer_cursor: Cursor, inner_cursor: Cursor, body: F) -> Emit<'a, ()>
where
    F: FnOnce(LoopContext, LoopContext) -> Emit<'a, ()> + 'a,
{
    for_each(outer_cursor, move |outer_ctx| {
        for_each(inner_cursor, move |inner_ctx| body(outer_ctx, inner_ctx))
    })
}

/// Emit a triple-nested loop (for 3-way joins).
pub fn triple_loop<'a, F>(
    cursor1: Cursor,
    cursor2: Cursor,
    cursor3: Cursor,
    body: F,
) -> Emit<'a, ()>
where
    F: FnOnce(LoopContext, LoopContext, LoopContext) -> Emit<'a, ()> + 'a,
{
    for_each(cursor1, move |ctx1| {
        for_each(cursor2, move |ctx2| {
            for_each(cursor3, move |ctx3| body(ctx1, ctx2, ctx3))
        })
    })
}

// =============================================================================
// Conditional Control Flow
// =============================================================================

/// Emit a conditional branch in bytecode.
///
/// If the condition register is true (non-zero), executes `then_branch`,
/// otherwise executes `else_branch`.
///
/// # Note
/// This generates bytecode for runtime condition checking, not compile-time.
pub fn if_else<'a, T>(
    condition_reg: Reg,
    then_branch: Emit<'a, T>,
    else_branch: Emit<'a, T>,
) -> Emit<'a, T>
where
    T: Clone + 'a,
{
    alloc_label().flat_map(move |else_label| {
        alloc_label().flat_map(move |end_label| {
            // Jump to else if condition is false
            emit(InsnSpec::IfNot {
                reg: condition_reg,
                target: else_label,
                jump_if_null: true,
            })
            // Then branch
            .then(then_branch)
            .flat_map(move |then_result| {
                // Jump over else
                emit(InsnSpec::Goto { target: end_label })
                    // Else label
                    .then(bind_label(else_label))
                    // Else branch
                    .then(else_branch)
                    .flat_map(move |_else_result| {
                        // End label
                        bind_label(end_label).map(move |_| then_result)
                    })
            })
        })
    })
}

/// Emit a simple conditional (no else branch).
///
/// If the condition is true, executes the body. Otherwise skips it.
pub fn when_true<'a>(condition_reg: Reg, body: Emit<'a, ()>) -> Emit<'a, ()> {
    alloc_label().flat_map(move |skip_label| {
        emit(InsnSpec::IfNot {
            reg: condition_reg,
            target: skip_label,
            jump_if_null: true,
        })
        .then(body)
        .then(bind_label(skip_label))
    })
}

/// Emit a conditional that executes when the condition is false.
pub fn when_false<'a>(condition_reg: Reg, body: Emit<'a, ()>) -> Emit<'a, ()> {
    alloc_label().flat_map(move |skip_label| {
        emit(InsnSpec::If {
            reg: condition_reg,
            target: skip_label,
            jump_if_null: false,
        })
        .then(body)
        .then(bind_label(skip_label))
    })
}

/// Emit a null check with conditional execution.
///
/// If the register is NULL, executes `if_null`. Otherwise executes `if_not_null`.
pub fn null_check<'a, T: 'a>(
    reg: Reg,
    if_null: Emit<'a, T>,
    if_not_null: Emit<'a, T>,
) -> Emit<'a, T> {
    alloc_label().flat_map(move |not_null_label| {
        alloc_label().flat_map(move |end_label| {
            emit(InsnSpec::NotNull {
                reg,
                target: not_null_label,
            })
            .then(if_null)
            .flat_map(move |null_result| {
                emit(InsnSpec::Goto { target: end_label })
                    .then(bind_label(not_null_label))
                    .then(if_not_null)
                    .flat_map(move |_| bind_label(end_label).map(move |_| null_result))
            })
        })
    })
}

/// Emit a null check that skips the body if NULL.
pub fn skip_if_null<'a>(reg: Reg, body: Emit<'a, ()>) -> Emit<'a, ()> {
    alloc_label().flat_map(move |skip_label| {
        emit(InsnSpec::IsNull {
            reg,
            target: skip_label,
        })
        .then(body)
        .then(bind_label(skip_label))
    })
}

/// Emit a null check that skips the body if NOT NULL.
pub fn skip_if_not_null<'a>(reg: Reg, body: Emit<'a, ()>) -> Emit<'a, ()> {
    alloc_label().flat_map(move |skip_label| {
        emit(InsnSpec::NotNull {
            reg,
            target: skip_label,
        })
        .then(body)
        .then(bind_label(skip_label))
    })
}

// =============================================================================
// Early Exit / Break
// =============================================================================

/// Emit a jump to a label (for early exit).
pub fn jump_to<'a>(label: Label) -> Emit<'a, ()> {
    emit(InsnSpec::Goto { target: label })
}

/// Emit a conditional jump.
pub fn jump_if<'a>(condition_reg: Reg, target: Label) -> Emit<'a, ()> {
    emit(InsnSpec::If {
        reg: condition_reg,
        target,
        jump_if_null: false,
    })
}

/// Emit a conditional jump when false.
pub fn jump_if_not<'a>(condition_reg: Reg, target: Label) -> Emit<'a, ()> {
    emit(InsnSpec::IfNot {
        reg: condition_reg,
        target,
        jump_if_null: false,
    })
}

// =============================================================================
// Subroutines
// =============================================================================

/// Emit a subroutine pattern (Gosub...Return).
///
/// Allocates a return register, emits the subroutine body with a Return
/// at the end, then emits a Gosub to call it.
///
/// Returns the label of the subroutine start for direct calls.
pub fn subroutine<'a, F>(body: F) -> Emit<'a, (Label, Reg)>
where
    F: FnOnce(Reg) -> Emit<'a, ()> + 'a,
{
    alloc_reg().flat_map(|return_reg| {
        alloc_label().flat_map(move |sub_label| {
            alloc_label().flat_map(move |after_sub| {
                // Jump over subroutine definition
                emit(InsnSpec::Goto { target: after_sub })
                    // Subroutine start
                    .then(bind_label(sub_label))
                    // Subroutine body
                    .then(body(return_reg))
                    // Return
                    .then(emit(InsnSpec::Return {
                        return_reg,
                        can_fallthrough: false,
                    }))
                    // After subroutine
                    .then(bind_label(after_sub))
                    .map(move |_| (sub_label, return_reg))
            })
        })
    })
}

/// Call a subroutine.
pub fn call_subroutine<'a>(sub_label: Label, return_reg: Reg) -> Emit<'a, ()> {
    emit(InsnSpec::Gosub {
        target: sub_label,
        return_reg,
    })
}

// =============================================================================
// Coroutines
// =============================================================================

/// Emit a coroutine pattern.
///
/// A coroutine is like a generator - it yields values and can be resumed.
/// Used for subqueries that produce rows.
pub fn coroutine<'a, F>(body: F) -> Emit<'a, Reg>
where
    F: FnOnce(Reg, Label) -> Emit<'a, ()> + 'a,
{
    alloc_reg().flat_map(|yield_reg| {
        alloc_label().flat_map(move |start_label| {
            alloc_label().flat_map(move |end_label| {
                // Initialize coroutine
                emit(InsnSpec::InitCoroutine {
                    yield_reg,
                    jump_on_init: end_label,
                    start_label,
                })
                // Coroutine start
                .then(bind_label(start_label))
                // Body (can yield multiple times)
                .then(body(yield_reg, end_label))
                // End coroutine
                .then(emit(InsnSpec::EndCoroutine { yield_reg }))
                // After coroutine setup
                .then(bind_label(end_label))
                .map(move |_| yield_reg)
            })
        })
    })
}

/// Yield from a coroutine.
pub fn yield_value<'a>(yield_reg: Reg, resume_label: Label) -> Emit<'a, ()> {
    emit(InsnSpec::Yield {
        yield_reg,
        resume_label,
    })
}

// =============================================================================
// Once Pattern
// =============================================================================

/// Emit a "once" pattern - code that only executes on the first iteration.
///
/// Useful for uncorrelated subqueries that should only be evaluated once.
pub fn once<'a, T: 'a>(body: Emit<'a, T>) -> Emit<'a, T> {
    alloc_label().flat_map(move |skip_label| {
        emit(InsnSpec::Once { target: skip_label })
            .then(body)
            .before(bind_label(skip_label))
    })
}

// =============================================================================
// Sorter Loops
// =============================================================================

/// Emit a loop over a sorter.
///
/// First sorts the data, then iterates through sorted results.
pub fn sorter_loop<'a, F>(sorter: Cursor, body: F) -> Emit<'a, ()>
where
    F: FnOnce(LoopContext) -> Emit<'a, ()> + 'a,
{
    alloc_loop_labels().flat_map(move |labels| {
        let ctx = LoopContext::new(sorter, labels);

        // Sort the data, jump to end if empty
        emit(InsnSpec::SorterSort {
            cursor: sorter,
            if_empty: labels.end,
        })
        // Loop start
        .then(bind_label(labels.start))
        // Body
        .then(body(ctx))
        // Next label
        .then(bind_label(labels.next))
        // Move to next sorted row
        .then(emit(InsnSpec::SorterNext {
            cursor: sorter,
            if_next: labels.start,
        }))
        // End label
        .then(bind_label(labels.end))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::monadic::types::test_helpers::TestEnv;

    #[test]
    fn test_for_each_structure() {
        let env = TestEnv::new();
        let cursor = Cursor(0);

        // Simple loop that does nothing
        let computation = for_each(cursor, |_ctx| Emit::pure(()));
        let (_, state) = env.run(computation).unwrap();

        // Should have: Rewind, (body), Next, plus label bindings handled internally
        // The instructions should include Rewind and Next
        let has_rewind = state
            .instructions
            .iter()
            .any(|i| matches!(i, InsnSpec::Rewind { .. }));
        let has_next = state
            .instructions
            .iter()
            .any(|i| matches!(i, InsnSpec::Next { .. }));

        assert!(has_rewind, "Loop should have Rewind instruction");
        assert!(has_next, "Loop should have Next instruction");
    }

    #[test]
    fn test_loop_labels_allocated() {
        let env = TestEnv::new();
        let cursor = Cursor(0);

        let computation = for_each(cursor, |ctx| {
            // Verify we have access to all three labels
            let _start = ctx.labels.start;
            let _next = ctx.labels.next;
            let _end = ctx.labels.end;
            Emit::pure(())
        });

        let (_, state) = env.run(computation).unwrap();

        // All labels should be resolved after the loop completes
        assert!(state.labels.all_resolved());
    }

    #[test]
    fn test_once_pattern() {
        let env = TestEnv::new();
        let computation = once(emit(InsnSpec::Noop));
        let (_, state) = env.run(computation).unwrap();

        // Should have Once instruction followed by Noop
        let has_once = state
            .instructions
            .iter()
            .any(|i| matches!(i, InsnSpec::Once { .. }));
        assert!(has_once, "Should have Once instruction");
    }

    #[test]
    fn test_nested_loop() {
        let env = TestEnv::new();
        let outer = Cursor(0);
        let inner = Cursor(1);

        let computation = nested_loop(outer, inner, |_outer_ctx, _inner_ctx| Emit::pure(()));
        let (_, state) = env.run(computation).unwrap();

        // Should have two Rewind and two Next instructions
        let rewind_count = state
            .instructions
            .iter()
            .filter(|i| matches!(i, InsnSpec::Rewind { .. }))
            .count();
        let next_count = state
            .instructions
            .iter()
            .filter(|i| matches!(i, InsnSpec::Next { .. }))
            .count();

        assert_eq!(
            rewind_count, 2,
            "Nested loop should have 2 Rewind instructions"
        );
        assert_eq!(next_count, 2, "Nested loop should have 2 Next instructions");
    }
}
