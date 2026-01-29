//! # Examples of Monadic Bytecode Emission
//!
//! This module demonstrates how to use the monadic abstraction for various
//! translation patterns. Each example shows how the monadic approach enables
//! declarative, composable bytecode generation.
//!
//! ## Key Patterns Demonstrated
//!
//! 1. **Simple Expression Translation** - Loading constants, arithmetic
//! 2. **Control Flow** - Conditional jumps, forward labels
//! 3. **Loops** - Table scan patterns with iteration
//! 4. **Composition** - Building complex patterns from simple building blocks

#![allow(dead_code, unused_imports)]

use super::*;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::vdbe::BranchOffset;
use crate::Result;

// =============================================================================
// Example 1: Simple Arithmetic Expression
// =============================================================================
//
// Translating: SELECT 1 + 2 * 3
//
// Traditional imperative approach:
// ```
// fn translate_arithmetic_imperative(program: &mut ProgramBuilder) -> Result<usize> {
//     let reg1 = program.alloc_register();
//     program.emit_insn(Insn::Integer { value: 2, dest: reg1 });
//     let reg2 = program.alloc_register();
//     program.emit_insn(Insn::Integer { value: 3, dest: reg2 });
//     let reg_mul = program.alloc_register();
//     program.emit_insn(Insn::Multiply { lhs: reg1, rhs: reg2, dest: reg_mul });
//     let reg3 = program.alloc_register();
//     program.emit_insn(Insn::Integer { value: 1, dest: reg3 });
//     let reg_result = program.alloc_register();
//     program.emit_insn(Insn::Add { lhs: reg3, rhs: reg_mul, dest: reg_result });
//     Ok(reg_result)
// }
// ```

/// Monadic approach for arithmetic expression
fn translate_arithmetic_monadic() -> impl Emit<Output = usize> {
    // First compute 2 * 3
    integer_new_reg(2).then(|reg2| {
        integer_new_reg(3).then(move |reg3| {
            alloc_reg().then(move |reg_mul| {
                multiply(reg2, reg3, reg_mul).then(move |_| {
                    // Then compute 1 + result
                    integer_new_reg(1).then(move |reg1| {
                        alloc_reg().then(move |reg_result| {
                            add(reg1, reg_mul, reg_result).map(move |_| reg_result)
                        })
                    })
                })
            })
        })
    })
}

// =============================================================================
// Example 2: Conditional Expression (CASE WHEN)
// =============================================================================
//
// Pattern:
//   - Check condition
//   - If false, jump to else branch
//   - Execute then branch, jump to end
//   - Execute else branch
//   - End label

/// Monadic approach for a simple conditional
fn translate_conditional(
    condition_reg: usize,
    then_value: i64,
    else_value: i64,
) -> impl Emit<Output = usize> {
    alloc_reg().then(move |result_reg| {
        with_forward_label(move |else_label| {
            with_forward_label(move |end_label| {
                // Jump to else if condition is false
                if_not(condition_reg, else_label, true)
                    // THEN branch
                    .and_then(integer(then_value, result_reg))
                    .and_then(goto(end_label))
                    // ELSE branch (label resolved here)
                    .and_then(resolve_label(else_label))
                    .and_then(integer(else_value, result_reg))
                // end_label resolved by with_forward_label
            })
        })
        .map(move |_| result_reg)
    })
}

// =============================================================================
// Example 3: Loop Structure
// =============================================================================
//
// Pattern:
//   - Initialize (rewind cursor)
//   - Loop body with iteration
//   - Next instruction jumps back to loop start

/// Demonstrates a simple loop pattern using forward labels
fn translate_simple_loop(
    cursor_id: usize,
    result_reg: usize,
    count_reg: usize,
) -> impl Emit<Output = ()> {
    with_forward_label(move |loop_end| {
        // Initialize count to 0
        integer(0, count_reg)
            .then(move |_| {
                // Emit Rewind - if empty, jump to end
                insn(Insn::Rewind {
                    cursor_id,
                    pc_if_empty: loop_end,
                })
            })
            .then(move |_| {
                with_forward_label(move |loop_start| {
                    // Mark loop start
                    resolve_label(loop_start)
                        .then(move |_| {
                            // Loop body: increment count
                            add(count_reg, count_reg, count_reg)
                        })
                        .then(move |_| {
                            // Next - if more rows, jump to loop_start
                            insn(Insn::Next {
                                cursor_id,
                                pc_if_next: loop_start,
                            })
                        })
                })
            })
        // loop_end resolved here automatically
    })
    .and_then(copy(count_reg, result_reg))
    .map(|_| ())
}

// =============================================================================
// Example 4: Composable Building Blocks
// =============================================================================

/// Load an integer constant into a new register - a basic building block
fn load_int(value: i64) -> impl Emit<Output = usize> {
    alloc_reg().then(move |reg| integer(value, reg).map(move |_| reg))
}

/// Emit a binary operation into a new register
fn emit_binary_op<F, E>(
    lhs: impl Emit<Output = usize>,
    rhs: impl Emit<Output = usize>,
    op: F,
) -> impl Emit<Output = usize>
where
    F: FnOnce(usize, usize, usize) -> E,
    E: Emit<Output = ()>,
{
    lhs.zip(rhs)
        .then(move |(l, r)| alloc_reg().then(move |dest| op(l, r, dest).map(move |_| dest)))
}

/// Demonstrates composition: (1 + 2) * (3 + 4)
fn complex_arithmetic() -> impl Emit<Output = usize> {
    // First compute 1 + 2
    let sum1 = emit_binary_op(load_int(1), load_int(2), |l, r, d| add(l, r, d));
    // Then compute 3 + 4
    let sum2 = emit_binary_op(load_int(3), load_int(4), |l, r, d| add(l, r, d));
    // Finally multiply the results
    emit_binary_op(sum1, sum2, |l, r, d| multiply(l, r, d))
}

// =============================================================================
// Example 5: Conditional Emission
// =============================================================================

/// Conditionally emit bytecode based on a compile-time decision
fn emit_with_optional_limit(
    has_limit: bool,
    limit_value: i64,
) -> impl Emit<Output = Option<usize>> {
    when(has_limit, move || {
        alloc_reg().then(move |limit_reg| integer(limit_value, limit_reg).map(move |_| limit_reg))
    })
}

/// Choose between two emission paths
fn emit_value_or_null(use_value: bool, value: i64) -> impl Emit<Output = usize> {
    alloc_reg().then(move |reg| {
        if_else(use_value, move || integer(value, reg), move || null(reg)).map(move |_| reg)
    })
}

// =============================================================================
// Example 6: Sequence of Operations
// =============================================================================

/// Emit multiple integers into consecutive registers
fn emit_int_sequence(values: Vec<i64>) -> impl Emit<Output = Vec<usize>> {
    sequence(
        values
            .into_iter()
            .map(|v| alloc_reg().then(move |r| integer(v, r).map(move |_| r)))
            .collect(),
    )
}

/// Emit a series of operations using for_each
fn emit_sum_of_integers(values: Vec<i64>) -> impl Emit<Output = usize> {
    // First register for accumulator, initialized to 0
    alloc_reg().then(|acc| {
        integer(0, acc)
            .then(move |_| {
                // Emit each value and add to accumulator
                for_each(values.into_iter(), move |v| {
                    alloc_reg().then(move |temp| integer(v, temp).and_then(add(acc, temp, acc)))
                })
            })
            .map(move |_| acc)
    })
}

// =============================================================================
// Example 7: Rewind-Next Loop Pattern
// =============================================================================

/// A reusable loop pattern that iterates over a cursor
fn cursor_loop<F, E>(cursor_id: usize, body: F) -> impl Emit<Output = ()>
where
    F: FnOnce() -> E,
    E: Emit<Output = ()>,
{
    with_forward_label(move |done| {
        insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: done,
        })
        .then(move |_| {
            with_forward_label(move |loop_start| {
                resolve_label(loop_start).and_then(body()).then(move |_| {
                    insn(Insn::Next {
                        cursor_id,
                        pc_if_next: loop_start,
                    })
                })
            })
        })
    })
    .map(|_| ())
}

// =============================================================================
// Benefits Summary
// =============================================================================
//
// 1. **Composability**: Small building blocks combine into complex structures
//    - `load_int`, `emit_binary_op` are reusable
//
// 2. **Declarative**: Code describes WHAT to emit, not HOW
//    - Label management is handled automatically by `with_forward_label`
//    - Register allocation flows naturally through the computation
//
// 3. **Type Safety**: The type system ensures correct composition
//    - `Emit<Output = usize>` clearly indicates what's produced
//    - Can't accidentally use a label as a register
//
// 4. **Zero Cost**: Everything inlines away
//    - `#[inline(always)]` on all combinators
//    - Final code is equivalent to hand-written imperative code
//
// 5. **Lazy Evaluation**: Nothing executes until `.run()` is called
//    - Can build up computations and decide whether to run them
//    - Enables optimizations like dead code elimination
//
// 6. **Better Error Messages**: Structured error handling
//    - Errors propagate naturally through the monad
//    - Can add context with `.map_err()`

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
                num_cursors: 2,
                approx_num_insns: 50,
                approx_num_labels: 10,
            },
        )
    }

    #[test]
    fn test_arithmetic_monadic() {
        let mut program = test_program();
        let result = translate_arithmetic_monadic().run(&mut program).unwrap();

        // Result should be a valid register
        assert!(result > 0);

        // Should have emitted: 3 integers + 1 multiply + 1 add = 5 instructions
        assert_eq!(program.insns.len(), 5);
    }

    #[test]
    fn test_composition() {
        let mut program = test_program();

        // Test that building blocks compose correctly
        let computation = alloc_reg()
            .then(|r1| alloc_reg().map(move |r2| (r1, r2)))
            .then(|(r1, r2)| {
                integer(1, r1)
                    .and_then(integer(2, r2))
                    .map(move |_| (r1, r2))
            });

        let (r1, r2) = computation.run(&mut program).unwrap();
        assert_eq!(r1, 1);
        assert_eq!(r2, 2);
        assert_eq!(program.insns.len(), 2);
    }

    #[test]
    fn test_conditional_when() {
        let mut program1 = test_program();
        let result1 = emit_with_optional_limit(true, 10)
            .run(&mut program1)
            .unwrap();
        assert!(result1.is_some());
        assert_eq!(program1.insns.len(), 1); // One integer instruction

        let mut program2 = test_program();
        let result2 = emit_with_optional_limit(false, 10)
            .run(&mut program2)
            .unwrap();
        assert!(result2.is_none());
        assert_eq!(program2.insns.len(), 0); // No instructions
    }

    #[test]
    fn test_sequence() {
        let mut program = test_program();
        let regs = emit_int_sequence(vec![1, 2, 3]).run(&mut program).unwrap();

        assert_eq!(regs.len(), 3);
        assert_eq!(program.insns.len(), 3);
    }

    #[test]
    fn test_complex_arithmetic() {
        let mut program = test_program();
        let result = complex_arithmetic().run(&mut program).unwrap();

        assert!(result > 0);
        // 4 integers + 2 adds + 1 multiply = 7 instructions
        assert_eq!(program.insns.len(), 7);
    }
}
