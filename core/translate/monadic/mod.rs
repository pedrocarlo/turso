//! Monadic bytecode emitter for declarative instruction generation.
//!
//! This module provides a monadic approach to bytecode generation that
//! replaces imperative state threading with composable, declarative computations.
//!
//! # Overview
//!
//! The traditional emitter uses mutable `ProgramBuilder` and `TranslateCtx`
//! passed through function calls. This monadic emitter instead uses:
//!
//! - `Emit<T>`: A monad representing a computation that produces `T`
//! - Typed resource references: `Reg`, `Cursor`, `Label` instead of raw `usize`
//! - Declarative combinators: `for_each`, `if_else`, `once`, etc.
//!
//! # Key Components
//!
//! - [`types`]: Core types (`Emit`, `EmitEnv`, `EmitState`, resource refs)
//! - [`insn`]: Declarative instruction specification (`InsnSpec`)
//! - [`alloc`]: Resource allocation primitives
//! - [`control`]: Control flow combinators
//! - [`macros`]: Do-notation macro for readable composition
//!
//! # Example
//!
//! ```ignore
//! use turso_core::emit_do;
//! use turso_core::translate::monadic::*;
//!
//! // Simple SELECT emitter
//! fn emit_simple_select(table: &Table) -> Emit<'_, ()> {
//!     emit_do! {
//!         cursor <- open_read(table);
//!         result_reg <- alloc_reg();
//!
//!         _ <- for_each(cursor, |ctx| emit_do! {
//!             val <- read_column(ctx.cursor, 0);
//!             _ <- emit_copy(val, result_reg);
//!             _ <- emit_result_row(result_reg, 1);
//!             pure(())
//!         });
//!
//!         _ <- emit_halt();
//!         pure(())
//!     }
//! }
//! ```
//!
//! # Monad Laws
//!
//! The `Emit` monad satisfies the three monad laws:
//!
//! 1. **Left identity**: `pure(a).flat_map(f) ≡ f(a)`
//! 2. **Right identity**: `m.flat_map(pure) ≡ m`
//! 3. **Associativity**: `m.flat_map(f).flat_map(g) ≡ m.flat_map(|x| f(x).flat_map(g))`
//!
//! # Migration Strategy
//!
//! This module can coexist with the imperative emitter. Use `EmitState::from_program_builder`
//! and `EmitState::sync_to_program_builder` to bridge between the two approaches.

pub mod alloc;
pub mod control;
pub mod insn;
#[macro_use]
pub mod macros;
pub mod types;

// Re-export commonly used items
// These are intentionally exported for users of the module
#[allow(unused_imports)]
pub use alloc::{
    alloc_cursor, alloc_ephemeral_cursor, alloc_hash_table, alloc_index_cursor, alloc_label,
    alloc_loop_labels, alloc_pseudo_cursor, alloc_reg, alloc_reg_int, alloc_reg_null,
    alloc_reg_real, alloc_reg_string, alloc_regs, alloc_regs_null, alloc_sorter_cursor,
    alloc_table_cursor, bind_label, current_pos, emit, emit_all, emit_column, emit_copy, emit_goto,
    emit_halt, emit_halt_error, emit_int, emit_null, emit_nulls, emit_result_row,
    emit_result_row_range, emit_rowid, emit_string, here, nesting_depth, read_column, read_rowid,
    scoped,
};

#[allow(unused_imports)]
pub use control::{
    call_subroutine, coroutine, for_each, for_each_rev, for_each_with_labels, if_else, jump_if,
    jump_if_not, jump_to, nested_loop, null_check, once, skip_if_not_null, skip_if_null,
    sorter_loop, subroutine, triple_loop, when_false, when_true, yield_value, LoopContext,
};

#[allow(unused_imports)]
pub use insn::InsnSpec;

#[allow(unused_imports)]
pub use types::{
    ask, for_each_item, get, if_then_else, match_option, modify, sequence, traverse, Cursor,
    CursorKind, CursorTable, Emit, EmitEnv, EmitState, HashTableId, InsnPos, Label, LabelTable,
    LoopLabels, Reg, RegRange,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::monadic::types::test_helpers::TestEnv;

    // =========================================================================
    // Monad Law Tests
    // =========================================================================

    #[test]
    fn test_monad_left_identity() {
        // Left identity: pure(a).flat_map(f) ≡ f(a)
        let env = TestEnv::new();
        let a = 42i64;
        let f = |x: i64| Emit::pure(x * 2);

        let lhs = Emit::pure(a).flat_map(f);
        let rhs = f(a);

        let (lhs_result, _) = env.run(lhs).unwrap();
        let env2 = TestEnv::new();
        let (rhs_result, _) = env2.run(rhs).unwrap();

        assert_eq!(lhs_result, rhs_result);
    }

    #[test]
    fn test_monad_right_identity() {
        // Right identity: m.flat_map(pure) ≡ m
        let env1 = TestEnv::new();
        let env2 = TestEnv::new();

        // We can't directly compare Emit values, so we compare results
        let (lhs_result, lhs_state) = env1.run(alloc_reg().flat_map(Emit::pure)).unwrap();
        let (rhs_result, rhs_state) = env2.run(alloc_reg()).unwrap();

        // Both should allocate a register with the same index
        assert_eq!(lhs_result.index(), rhs_result.index());
        // And both should advance the register counter the same amount
        assert_eq!(lhs_state.next_register, rhs_state.next_register);
    }

    #[test]
    fn test_monad_associativity() {
        // Associativity: m.flat_map(f).flat_map(g) ≡ m.flat_map(|x| f(x).flat_map(g))
        let env1 = TestEnv::new();
        let env2 = TestEnv::new();

        let m = || alloc_reg();
        let f = |r: Reg| alloc_reg_int(r.index() as i64);
        let g = |r: Reg| Emit::pure(r.index() * 2);

        // LHS: m.flat_map(f).flat_map(g)
        let lhs = m().flat_map(f).flat_map(g);

        // RHS: m.flat_map(|x| f(x).flat_map(g))
        let rhs = m().flat_map(|x| f(x).flat_map(g));

        let (lhs_result, lhs_state) = env1.run(lhs).unwrap();
        let (rhs_result, rhs_state) = env2.run(rhs).unwrap();

        assert_eq!(lhs_result, rhs_result);
        assert_eq!(lhs_state.next_register, rhs_state.next_register);
        assert_eq!(lhs_state.instructions.len(), rhs_state.instructions.len());
    }

    // =========================================================================
    // Functor Law Tests
    // =========================================================================

    #[test]
    fn test_functor_identity() {
        // Identity: m.map(id) ≡ m
        let env1 = TestEnv::new();
        let env2 = TestEnv::new();

        let (lhs_result, _) = env1.run(alloc_reg().map(|x| x)).unwrap();
        let (rhs_result, _) = env2.run(alloc_reg()).unwrap();

        assert_eq!(lhs_result.index(), rhs_result.index());
    }

    #[test]
    fn test_functor_composition() {
        // Composition: m.map(f).map(g) ≡ m.map(|x| g(f(x)))
        let env1 = TestEnv::new();
        let env2 = TestEnv::new();

        let f = |r: Reg| r.index() * 2;
        let g = |x: usize| x + 1;

        let lhs = alloc_reg().map(f).map(g);
        let rhs = alloc_reg().map(|x| g(f(x)));

        let (lhs_result, _) = env1.run(lhs).unwrap();
        let (rhs_result, _) = env2.run(rhs).unwrap();

        assert_eq!(lhs_result, rhs_result);
    }

    // =========================================================================
    // Applicative Tests
    // =========================================================================

    #[test]
    fn test_zip() {
        let env = TestEnv::new();
        let computation = alloc_reg().zip(alloc_reg());

        let ((r1, r2), state) = env.run(computation).unwrap();

        assert_ne!(r1.index(), r2.index());
        assert_eq!(state.next_register, 3); // Started at 1, allocated 2
    }

    #[test]
    fn test_zip3() {
        let env = TestEnv::new();
        let computation = alloc_reg().zip3(alloc_reg(), alloc_reg());

        let ((r1, r2, r3), state) = env.run(computation).unwrap();

        assert_ne!(r1.index(), r2.index());
        assert_ne!(r2.index(), r3.index());
        assert_eq!(state.next_register, 4);
    }

    #[test]
    fn test_sequence() {
        let env = TestEnv::new();
        let computations = vec![alloc_reg(), alloc_reg(), alloc_reg()];

        let (regs, state) = env.run(sequence(computations)).unwrap();

        assert_eq!(regs.len(), 3);
        assert_eq!(state.next_register, 4);

        // All registers should be distinct
        let indices: Vec<_> = regs.iter().map(|r| r.index()).collect();
        assert_eq!(indices, vec![1, 2, 3]);
    }

    #[test]
    fn test_traverse() {
        let env = TestEnv::new();
        let items = vec![10i64, 20, 30];

        let computation = traverse(items, |value| alloc_reg_int(value));

        let (regs, state) = env.run(computation).unwrap();

        assert_eq!(regs.len(), 3);
        assert_eq!(state.instructions.len(), 3); // 3 Integer instructions
    }

    // =========================================================================
    // Control Flow Tests
    // =========================================================================

    #[test]
    fn test_for_each_generates_loop_structure() {
        let env = TestEnv::new();
        let cursor = Cursor(0);

        let computation = for_each(cursor, |_ctx| Emit::pure(()));

        let (_, state) = env.run(computation).unwrap();

        // Should have Rewind and Next
        let has_rewind = state
            .instructions
            .iter()
            .any(|i| matches!(i, InsnSpec::Rewind { .. }));
        let has_next = state
            .instructions
            .iter()
            .any(|i| matches!(i, InsnSpec::Next { .. }));

        assert!(has_rewind);
        assert!(has_next);

        // All labels should be resolved
        assert!(state.labels.all_resolved());
    }

    #[test]
    fn test_nested_loop_structure() {
        let env = TestEnv::new();
        let outer = Cursor(0);
        let inner = Cursor(1);

        let computation = nested_loop(outer, inner, |_o, _i| Emit::pure(()));

        let (_, state) = env.run(computation).unwrap();

        // Should have 2 Rewind and 2 Next instructions
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

        assert_eq!(rewind_count, 2);
        assert_eq!(next_count, 2);
    }

    #[test]
    fn test_once_pattern() {
        let env = TestEnv::new();
        let computation = once(emit(InsnSpec::Noop));

        let (_, state) = env.run(computation).unwrap();

        let has_once = state
            .instructions
            .iter()
            .any(|i| matches!(i, InsnSpec::Once { .. }));
        assert!(has_once);
    }

    // =========================================================================
    // Integration Tests
    // =========================================================================

    #[test]
    fn test_simple_query_pattern() {
        // Simulates: SELECT col0 FROM table
        let env = TestEnv::new();
        let cursor = Cursor(0);

        // Using functional composition
        let computation = alloc_reg().flat_map(move |result| {
            for_each(cursor, move |ctx| {
                read_column(ctx.cursor, 0)
                    .flat_map(move |val| emit_copy(val, result).then(emit_result_row(result, 1)))
            })
            .then(emit_halt())
            .map(move |_| result)
        });

        let (result_reg, state) = env.run(computation).unwrap();

        // Should have allocated registers
        assert!(result_reg.index() >= 1);

        // Should have instructions for: Rewind, Column, SCopy, ResultRow, Next, Halt
        let insn_types: Vec<&str> = state
            .instructions
            .iter()
            .map(|i| match i {
                InsnSpec::Rewind { .. } => "Rewind",
                InsnSpec::Column { .. } => "Column",
                InsnSpec::SCopy { .. } => "SCopy",
                InsnSpec::ResultRow { .. } => "ResultRow",
                InsnSpec::Next { .. } => "Next",
                InsnSpec::Halt { .. } => "Halt",
                _ => "Other",
            })
            .collect();

        assert!(insn_types.contains(&"Rewind"));
        assert!(insn_types.contains(&"Column"));
        assert!(insn_types.contains(&"ResultRow"));
        assert!(insn_types.contains(&"Next"));
        assert!(insn_types.contains(&"Halt"));
    }

    #[test]
    fn test_error_short_circuits() {
        use crate::error::LimboError;

        // This test needs to check state after error, so we create the env manually
        let env = TestEnv::new();
        let computation = alloc_reg()
            .flat_map(|_| Emit::fail(LimboError::InternalError("test error".into())))
            .flat_map(|_: ()| alloc_reg()); // This should not run

        let result = env.run(computation);
        assert!(result.is_err());
    }

    #[test]
    fn test_or_else_recovers() {
        use crate::error::LimboError;

        let env = TestEnv::new();
        let computation =
            Emit::<i32>::fail(LimboError::InternalError("test".into())).or_else(|_| Emit::pure(42));

        let (result, _) = env.run(computation).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_scoped_nesting_depth() {
        // Test using functional composition instead of macro
        let env = TestEnv::new();
        let computation = nesting_depth().flat_map(|depth1| {
            scoped(nesting_depth())
                .flat_map(move |depth2| nesting_depth().map(move |depth3| (depth1, depth2, depth3)))
        });

        let ((d1, d2, d3), _) = env.run(computation).unwrap();

        assert_eq!(d1, 0);
        assert_eq!(d2, 1);
        assert_eq!(d3, 0);
    }
}
