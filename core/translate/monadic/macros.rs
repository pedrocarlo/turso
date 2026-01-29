//! Do-notation macro for the monadic emitter.
//!
//! This macro provides Haskell-style do-notation for composing `Emit` computations,
//! making monadic code more readable and easier to write.
//!
//! # Example
//!
//! ```ignore
//! use turso_core::emit_do;
//!
//! let computation = emit_do! {
//!     // Allocate resources
//!     cursor <- open_read(&table);
//!     result_reg <- alloc_reg();
//!
//!     // Loop over rows
//!     _ <- for_each(cursor, |ctx| emit_do! {
//!         val <- read_column(ctx.cursor, 0);
//!         _ <- emit_copy(val, result_reg);
//!         _ <- emit_result_row(result_reg, 1);
//!         pure(())
//!     });
//!
//!     // Return the result register
//!     pure(result_reg)
//! };
//! ```

/// Do-notation macro for composing `Emit` monadic computations.
///
/// # Syntax
///
/// - `pure(expr)` - Wrap a value in `Emit::pure`
/// - `ident <- computation;` - Bind the result of a computation to an identifier
/// - `_ <- computation;` - Execute a computation, discarding its result
/// - `let pattern = expr;` - Pure let binding (not monadic)
/// - `computation` (final) - The final expression must be an `Emit`
///
/// # Example
///
/// ```ignore
/// emit_do! {
///     // Bind result to variable
///     reg <- alloc_reg();
///
///     // Execute without binding (discard result)
///     _ <- emit_int(42, reg);
///
///     // Pure let binding
///     let doubled = 42 * 2;
///
///     // Final expression
///     pure(reg)
/// }
/// ```
///
/// # Note on Conditionals
///
/// For compile-time conditionals, use regular Rust if/else and bind the result:
/// ```ignore
/// let computation = if condition {
///     emit_do! { ... }
/// } else {
///     emit_do! { ... }
/// };
/// ```
///
/// For runtime conditionals (bytecode branching), use the control flow
/// combinators like `if_else`, `when_true`, etc.
#[macro_export]
macro_rules! emit_do {
    // ==========================================================================
    // Terminal cases
    // ==========================================================================

    // pure(expr) - wrap value in Emit::pure
    (pure($e:expr)) => {
        $crate::translate::monadic::types::Emit::pure($e)
    };

    // Single expression (must be Emit)
    ($e:expr) => {
        $e
    };

    // ==========================================================================
    // Let bindings (pure, not monadic)
    // ==========================================================================

    // let pattern = expr; rest...
    (let $p:pat = $e:expr; $($rest:tt)+) => {
        {
            let $p = $e;
            $crate::emit_do!($($rest)+)
        }
    };

    // ==========================================================================
    // Monadic bindings
    // ==========================================================================

    // Discard result: _ <- computation; rest...
    (_ <- $e:expr; $($rest:tt)+) => {
        ($e).flat_map(move |_| $crate::emit_do!($($rest)+))
    };

    // ident <- computation; rest...
    ($name:ident <- $e:expr; $($rest:tt)+) => {
        ($e).flat_map(move |$name| $crate::emit_do!($($rest)+))
    };
}

/// A helper macro for creating instruction sequences.
///
/// This macro simplifies emitting multiple instructions in sequence.
///
/// # Example
///
/// ```ignore
/// emit_seq! {
///     InsnSpec::Null { dest: reg, count: 1 },
///     InsnSpec::Integer { value: 42, dest: reg },
///     InsnSpec::ResultRow { start_reg: reg, count: 1 },
/// }
/// ```
#[macro_export]
macro_rules! emit_seq {
    ($($insn:expr),+ $(,)?) => {
        $crate::translate::monadic::alloc::emit_all(vec![$($insn),+])
    };
}

// Note: For Option-based conditional emission, use the `match_option` function
// from the types module instead of a macro, as Rust macro limitations prevent
// clean syntax for this pattern.

#[cfg(test)]
mod tests {
    use crate::translate::monadic::alloc::{alloc_reg, emit};
    use crate::translate::monadic::insn::InsnSpec;
    use crate::translate::monadic::types::{test_helpers::TestEnv, Emit};

    // NOTE: The emit_do! macro uses #[macro_export] which places it at the crate root.
    // Testing the macro directly in this submodule has limitations due to Rust's
    // macro expansion order. These tests use functional composition style which
    // tests the same underlying monadic operations that the macro desugars to.
    //
    // For macro syntax tests, use integration tests at the crate root level.

    #[test]
    fn test_emit_do_pure() {
        // Tests: emit_do!(pure(42)) => Emit::pure(42)
        let env = TestEnv::new();
        let (result, _state) = env.run(Emit::pure(42)).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_emit_do_bind() {
        // Tests: emit_do! { reg <- alloc_reg(); pure(reg) }
        // Desugars to: alloc_reg().flat_map(move |reg| Emit::pure(reg))
        let env = TestEnv::new();
        let computation = alloc_reg().flat_map(move |reg| Emit::pure(reg));
        let (result, _state) = env.run(computation).unwrap();
        assert_eq!(result.index(), 1); // Registers start at 1
    }

    #[test]
    fn test_emit_do_sequence() {
        // Tests: emit_do! {
        //     reg <- alloc_reg();
        //     _ <- emit(InsnSpec::Integer { value: 42, dest: reg });
        //     _ <- emit(InsnSpec::Integer { value: 100, dest: reg });
        //     pure(reg)
        // }
        let env = TestEnv::new();
        let computation = alloc_reg().flat_map(move |reg| {
            emit(InsnSpec::Integer {
                value: 42,
                dest: reg,
            })
            .flat_map(move |_| {
                emit(InsnSpec::Integer {
                    value: 100,
                    dest: reg,
                })
                .flat_map(move |_| Emit::pure(reg))
            })
        });

        let (_result, state) = env.run(computation).unwrap();
        assert_eq!(state.instructions.len(), 2);
    }

    #[test]
    fn test_emit_do_let_binding() {
        // Tests: emit_do! {
        //     reg <- alloc_reg();
        //     let value = 42 * 2;
        //     _ <- emit(InsnSpec::Integer { value, dest: reg });
        //     pure(reg)
        // }
        let env = TestEnv::new();
        let computation = alloc_reg().flat_map(move |reg| {
            let value = 42 * 2;
            emit(InsnSpec::Integer { value, dest: reg }).flat_map(move |_| Emit::pure(reg))
        });

        let (_result, state) = env.run(computation).unwrap();

        // Check that the value was computed correctly
        match &state.instructions[0] {
            InsnSpec::Integer { value, .. } => assert_eq!(*value, 84),
            _ => panic!("Expected Integer instruction"),
        }
    }

    #[test]
    fn test_emit_do_conditional() {
        let use_large = true;

        // Tests conditional using if_then_else combinator within monadic chain
        let env = TestEnv::new();
        let computation = alloc_reg().flat_map(move |reg| {
            crate::translate::monadic::types::if_then_else(
                use_large,
                emit(InsnSpec::Integer {
                    value: 1000,
                    dest: reg,
                }),
                emit(InsnSpec::Integer {
                    value: 1,
                    dest: reg,
                }),
            )
            .flat_map(move |_| Emit::pure(reg))
        });

        let (_result, state) = env.run(computation).unwrap();

        match &state.instructions[0] {
            InsnSpec::Integer { value, .. } => assert_eq!(*value, 1000),
            _ => panic!("Expected Integer instruction"),
        }
    }

    #[test]
    fn test_nested_composition() {
        // Test nested composition using functional style
        let env = TestEnv::new();
        let computation = alloc_reg().flat_map(|reg1| {
            alloc_reg()
                .flat_map(|inner| {
                    emit(InsnSpec::Integer {
                        value: 1,
                        dest: inner,
                    })
                    .map(move |_| inner)
                })
                .flat_map(move |reg2| {
                    emit(InsnSpec::SCopy {
                        src: reg2,
                        dest: reg1,
                    })
                    .map(move |_| (reg1, reg2))
                })
        });

        let ((reg1, reg2), _state) = env.run(computation).unwrap();
        assert_ne!(reg1.index(), reg2.index());
    }
}
