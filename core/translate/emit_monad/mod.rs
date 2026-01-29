// Allow dead code since this is a new API that hasn't been integrated yet.
// TODO: Remove this once the API is being used in the translation code.
#![allow(dead_code)]

//! # Monadic Zero-Cost Abstraction for Bytecode Emission
//!
//! This module provides a declarative, lazy, composable approach to bytecode generation
//! using a monadic pattern that compiles to zero-overhead code.
//!
//! See [`emit_monad_examples`] (test module) for comprehensive usage examples.
//!
//! ## Design Philosophy
//!
//! The traditional imperative approach to bytecode emission looks like:
//! ```ignore
//! fn translate_something(program: &mut ProgramBuilder, ...) -> Result<()> {
//!     let reg = program.alloc_register();
//!     program.emit_insn(Insn::Integer { value: 42, dest: reg });
//!     let label = program.allocate_label();
//!     program.emit_insn(Insn::Goto { target_pc: label });
//!     // ... more imperative code
//!     program.resolve_label(label, program.offset());
//!     Ok(())
//! }
//! ```
//!
//! The monadic approach transforms this into:
//! ```ignore
//! fn translate_something() -> impl Emit<Output = ()> {
//!     integer(42)
//!         .then(|reg| {
//!             with_label(|label| {
//!                 goto(label)
//!                     .then(|_| some_loop_body())
//!                     .then(|_| resolve(label))
//!             })
//!         })
//! }
//! ```
//!
//! ## Key Benefits
//!
//! 1. **Zero-cost abstraction**: All monadic combinators inline and optimize away
//! 2. **Declarative**: Describe the bytecode structure, not the emission process
//! 3. **Lazy**: Nothing executes until `.run(program)` is called
//! 4. **Composable**: Small pieces combine into complex structures
//! 5. **Type-safe**: Register types and label scopes are tracked at compile time
//!
//! ## Core Concepts
//!
//! - `Emit<T>`: A computation that, when run, emits bytecode and produces a value of type T
//! - `pure(v)`: Lift a value into the Emit context without emitting anything
//! - `.then(f)`: Monadic bind - sequence computations, passing results forward
//! - `.map(f)`: Transform the output without additional emission
//! - `with_label(f)`: Create a scoped label that's resolved at the end
//!
//! ## Implementation Strategy
//!
//! The implementation uses Rust's zero-cost abstraction capabilities:
//! - Traits with associated types for the monad operations
//! - Generic structs that encode the computation structure
//! - `#[inline(always)]` to ensure everything gets inlined
//! - The final `.run()` call collapses the entire structure into direct calls
//!
//! ## Loop Abstractions
//!
//! For loop patterns, see the [`loop_emit`] module which provides chumsky-inspired
//! iteration combinators:
//!
//! ```ignore
//! // Simple cursor loop
//! cursor_loop(cursor_id, |ctx| {
//!     column(ctx.cursor_id, 0, dest_reg)
//! })
//! .emit_all()
//! .run(&mut program)?;
//!
//! // Collect results from each iteration
//! cursor_loop(cursor_id, |ctx| column(ctx.cursor_id, 0, temp_reg).map(move |_| temp_reg))
//!     .collect()
//!     .run(&mut program)?;
//!
//! // Static iteration over known items
//! static_iter(0..num_columns, |col_idx| {
//!     column(cursor_id, col_idx, base_reg + col_idx)
//! })
//! .emit_all()
//! .run(&mut program)?;
//! ```

#[cfg(test)]
mod emit_monad_examples;

pub mod loop_emit;

// Re-export commonly used loop_emit types and functions
#[allow(unused_imports)]
pub use loop_emit::{
    cursor_loop, generic_loop, nested_loop, reverse_cursor_loop, sorter_loop, static_iter,
    CursorLoop, GenericLoop, LoopContext, LoopEmit, NestedLoop, ReverseCursorLoop, SorterLoop,
    StaticIter,
};

use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::vdbe::BranchOffset;
use crate::Result;

// =============================================================================
// Core Trait: Emit<T>
// =============================================================================

/// A computation that emits bytecode instructions and produces a value.
///
/// This is the core abstraction - an `Emit<T>` represents a deferred computation
/// that will emit some bytecode and produce a value of type `T` when executed.
///
/// The trait is designed for zero-cost abstraction:
/// - All methods are `#[inline(always)]`
/// - Implementations are generic structs that inline completely
/// - No dynamic dispatch or heap allocation
pub trait Emit: Sized {
    /// The type of value produced when this computation is run.
    type Output;

    /// Execute the computation, emitting bytecode and returning the result.
    ///
    /// This is the only method that actually interacts with the ProgramBuilder.
    /// All other methods just build up a computation structure.
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output>;

    /// Monadic bind: sequence this computation with another that depends on its result.
    ///
    /// This is the fundamental composition operator. It runs `self`, passes the
    /// result to `f`, and runs the resulting computation.
    ///
    /// # Example
    /// ```ignore
    /// alloc_reg()
    ///     .then(|reg| emit_integer(42, reg))
    ///     .then(|_| alloc_reg())
    ///     .then(|reg2| emit_add(reg, reg2, reg2))
    /// ```
    #[inline(always)]
    fn then<F, E2>(self, f: F) -> Then<Self, F>
    where
        F: FnOnce(Self::Output) -> E2,
        E2: Emit,
    {
        Then { first: self, f }
    }

    /// Transform the output of this computation without emitting additional bytecode.
    ///
    /// # Example
    /// ```ignore
    /// alloc_reg().map(|reg| reg + 1)  // Offset the register
    /// ```
    #[inline(always)]
    fn map<F, B>(self, f: F) -> Map<Self, F>
    where
        F: FnOnce(Self::Output) -> B,
    {
        Map { emit: self, f }
    }

    /// Sequence two computations, discarding the first result.
    ///
    /// Equivalent to `.then(|_| other)` but clearer when the first result isn't needed.
    ///
    /// # Example
    /// ```ignore
    /// emit_null(reg1).and_then(emit_null(reg2))
    /// ```
    #[inline(always)]
    fn and_then<E2>(self, other: E2) -> AndThen<Self, E2>
    where
        E2: Emit,
    {
        AndThen {
            first: self,
            second: other,
        }
    }

    /// Combine with another computation, keeping both results as a tuple.
    ///
    /// # Example
    /// ```ignore
    /// alloc_reg().zip(alloc_reg())  // -> (reg1, reg2)
    /// ```
    #[inline(always)]
    fn zip<E2>(self, other: E2) -> Zip<Self, E2>
    where
        E2: Emit,
    {
        Zip {
            first: self,
            second: other,
        }
    }

    /// Conditionally emit bytecode based on a runtime value.
    ///
    /// # Example
    /// ```ignore
    /// pure(has_limit).then(|has_limit| {
    ///     if has_limit {
    ///         emit_limit_check(reg).map(|_| Some(label))
    ///     } else {
    ///         pure(None)
    ///     }
    /// })
    /// ```
    #[inline(always)]
    fn filter<F>(self, predicate: F) -> Filter<Self, F>
    where
        F: FnOnce(&Self::Output) -> bool,
    {
        Filter {
            emit: self,
            predicate,
        }
    }

    /// Add a side effect without changing the output.
    ///
    /// Useful for debugging or logging during development.
    #[inline(always)]
    fn inspect<F>(self, f: F) -> Inspect<Self, F>
    where
        F: FnOnce(&Self::Output),
    {
        Inspect { emit: self, f }
    }

    /// Convert errors using a mapping function.
    #[inline(always)]
    fn map_err<F>(self, f: F) -> MapErr<Self, F>
    where
        F: FnOnce(crate::error::LimboError) -> crate::error::LimboError,
    {
        MapErr { emit: self, f }
    }
}

// =============================================================================
// Combinator Structs (Zero-Cost Implementations)
// =============================================================================

/// The result of `emit.then(f)` - monadic bind.
pub struct Then<E, F> {
    first: E,
    f: F,
}

impl<E, F, E2> Emit for Then<E, F>
where
    E: Emit,
    F: FnOnce(E::Output) -> E2,
    E2: Emit,
{
    type Output = E2::Output;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.first.run(program)?;
        (self.f)(a).run(program)
    }
}

/// The result of `emit.map(f)` - functor map.
pub struct Map<E, F> {
    emit: E,
    f: F,
}

impl<E, F, B> Emit for Map<E, F>
where
    E: Emit,
    F: FnOnce(E::Output) -> B,
{
    type Output = B;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.emit.run(program)?;
        Ok((self.f)(a))
    }
}

/// The result of `emit.and_then(other)` - sequence, discarding first result.
pub struct AndThen<E1, E2> {
    first: E1,
    second: E2,
}

impl<E1, E2> Emit for AndThen<E1, E2>
where
    E1: Emit,
    E2: Emit,
{
    type Output = E2::Output;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        self.first.run(program)?;
        self.second.run(program)
    }
}

/// The result of `emit.zip(other)` - parallel composition.
pub struct Zip<E1, E2> {
    first: E1,
    second: E2,
}

impl<E1, E2> Emit for Zip<E1, E2>
where
    E1: Emit,
    E2: Emit,
{
    type Output = (E1::Output, E2::Output);

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.first.run(program)?;
        let b = self.second.run(program)?;
        Ok((a, b))
    }
}

/// The result of `emit.filter(predicate)`.
pub struct Filter<E, F> {
    emit: E,
    predicate: F,
}

impl<E, F> Emit for Filter<E, F>
where
    E: Emit,
    F: FnOnce(&E::Output) -> bool,
{
    type Output = Option<E::Output>;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let value = self.emit.run(program)?;
        if (self.predicate)(&value) {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

// =============================================================================
// Tuple Implementations (Chumsky-style)
// =============================================================================
//
// These allow combining multiple Emit values without nesting:
//
// Instead of:
//   alloc_reg().then(|r1| {
//       alloc_reg().then(|r2| {
//           alloc_label().then(|l1| { ... })
//       })
//   })
//
// Write:
//   (alloc_reg(), alloc_reg(), alloc_label()).then(|(r1, r2, l1)| { ... })

impl<E1, E2> Emit for (E1, E2)
where
    E1: Emit,
    E2: Emit,
{
    type Output = (E1::Output, E2::Output);

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.0.run(program)?;
        let b = self.1.run(program)?;
        Ok((a, b))
    }
}

impl<E1, E2, E3> Emit for (E1, E2, E3)
where
    E1: Emit,
    E2: Emit,
    E3: Emit,
{
    type Output = (E1::Output, E2::Output, E3::Output);

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.0.run(program)?;
        let b = self.1.run(program)?;
        let c = self.2.run(program)?;
        Ok((a, b, c))
    }
}

impl<E1, E2, E3, E4> Emit for (E1, E2, E3, E4)
where
    E1: Emit,
    E2: Emit,
    E3: Emit,
    E4: Emit,
{
    type Output = (E1::Output, E2::Output, E3::Output, E4::Output);

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.0.run(program)?;
        let b = self.1.run(program)?;
        let c = self.2.run(program)?;
        let d = self.3.run(program)?;
        Ok((a, b, c, d))
    }
}

impl<E1, E2, E3, E4, E5> Emit for (E1, E2, E3, E4, E5)
where
    E1: Emit,
    E2: Emit,
    E3: Emit,
    E4: Emit,
    E5: Emit,
{
    type Output = (E1::Output, E2::Output, E3::Output, E4::Output, E5::Output);

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.0.run(program)?;
        let b = self.1.run(program)?;
        let c = self.2.run(program)?;
        let d = self.3.run(program)?;
        let e = self.4.run(program)?;
        Ok((a, b, c, d, e))
    }
}

impl<E1, E2, E3, E4, E5, E6> Emit for (E1, E2, E3, E4, E5, E6)
where
    E1: Emit,
    E2: Emit,
    E3: Emit,
    E4: Emit,
    E5: Emit,
    E6: Emit,
{
    type Output = (
        E1::Output,
        E2::Output,
        E3::Output,
        E4::Output,
        E5::Output,
        E6::Output,
    );

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.0.run(program)?;
        let b = self.1.run(program)?;
        let c = self.2.run(program)?;
        let d = self.3.run(program)?;
        let e = self.4.run(program)?;
        let f = self.5.run(program)?;
        Ok((a, b, c, d, e, f))
    }
}

impl<E1, E2, E3, E4, E5, E6, E7> Emit for (E1, E2, E3, E4, E5, E6, E7)
where
    E1: Emit,
    E2: Emit,
    E3: Emit,
    E4: Emit,
    E5: Emit,
    E6: Emit,
    E7: Emit,
{
    type Output = (
        E1::Output,
        E2::Output,
        E3::Output,
        E4::Output,
        E5::Output,
        E6::Output,
        E7::Output,
    );

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.0.run(program)?;
        let b = self.1.run(program)?;
        let c = self.2.run(program)?;
        let d = self.3.run(program)?;
        let e = self.4.run(program)?;
        let f = self.5.run(program)?;
        let g = self.6.run(program)?;
        Ok((a, b, c, d, e, f, g))
    }
}

impl<E1, E2, E3, E4, E5, E6, E7, E8> Emit for (E1, E2, E3, E4, E5, E6, E7, E8)
where
    E1: Emit,
    E2: Emit,
    E3: Emit,
    E4: Emit,
    E5: Emit,
    E6: Emit,
    E7: Emit,
    E8: Emit,
{
    type Output = (
        E1::Output,
        E2::Output,
        E3::Output,
        E4::Output,
        E5::Output,
        E6::Output,
        E7::Output,
        E8::Output,
    );

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let a = self.0.run(program)?;
        let b = self.1.run(program)?;
        let c = self.2.run(program)?;
        let d = self.3.run(program)?;
        let e = self.4.run(program)?;
        let f = self.5.run(program)?;
        let g = self.6.run(program)?;
        let h = self.7.run(program)?;
        Ok((a, b, c, d, e, f, g, h))
    }
}

/// The result of `emit.inspect(f)`.
pub struct Inspect<E, F> {
    emit: E,
    f: F,
}

impl<E, F> Emit for Inspect<E, F>
where
    E: Emit,
    F: FnOnce(&E::Output),
{
    type Output = E::Output;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let value = self.emit.run(program)?;
        (self.f)(&value);
        Ok(value)
    }
}

/// The result of `emit.map_err(f)`.
pub struct MapErr<E, F> {
    emit: E,
    f: F,
}

impl<E, F> Emit for MapErr<E, F>
where
    E: Emit,
    F: FnOnce(crate::error::LimboError) -> crate::error::LimboError,
{
    type Output = E::Output;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        self.emit.run(program).map_err(self.f)
    }
}

// =============================================================================
// Primitive Emit Combinators
// =============================================================================

/// Lift a pure value into the Emit context without emitting any bytecode.
///
/// This is the monadic `return`/`pure` operation.
///
/// # Example
/// ```ignore
/// pure(42)  // Emit<Output = i32> that produces 42
/// ```
#[inline(always)]
pub fn pure<T>(value: T) -> Pure<T> {
    Pure { value }
}

pub struct Pure<T> {
    value: T,
}

impl<T> Emit for Pure<T> {
    type Output = T;

    #[inline(always)]
    fn run(self, _program: &mut ProgramBuilder) -> Result<Self::Output> {
        Ok(self.value)
    }
}

/// A computation that always fails with the given error.
#[inline(always)]
pub fn fail<T>(error: crate::error::LimboError) -> Fail<T> {
    Fail {
        error,
        _phantom: std::marker::PhantomData,
    }
}

pub struct Fail<T> {
    error: crate::error::LimboError,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Emit for Fail<T> {
    type Output = T;

    #[inline(always)]
    fn run(self, _program: &mut ProgramBuilder) -> Result<Self::Output> {
        Err(self.error)
    }
}

/// Lazily construct an Emit computation.
///
/// This is useful when you need to defer the construction of an Emit value.
///
/// # Example
/// ```ignore
/// lazy(|| {
///     if condition {
///         emit_something()
///     } else {
///         pure(())
///     }
/// })
/// ```
#[inline(always)]
pub fn lazy<F, E>(f: F) -> Lazy<F>
where
    F: FnOnce() -> E,
    E: Emit,
{
    Lazy { f }
}

pub struct Lazy<F> {
    f: F,
}

impl<F, E> Emit for Lazy<F>
where
    F: FnOnce() -> E,
    E: Emit,
{
    type Output = E::Output;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        (self.f)().run(program)
    }
}

// =============================================================================
// Register Allocation Primitives
// =============================================================================

/// Allocate a single register.
///
/// # Example
/// ```ignore
/// alloc_reg().then(|reg| emit_integer(42, reg))
/// ```
#[inline(always)]
pub fn alloc_reg() -> AllocReg {
    AllocReg { count: 1 }
}

/// Allocate multiple contiguous registers.
///
/// Returns the starting register number.
///
/// # Example
/// ```ignore
/// alloc_regs(3).then(|start| {
///     // Registers start, start+1, start+2 are now allocated
///     emit_make_record(start, 3, dest)
/// })
/// ```
#[inline(always)]
pub fn alloc_regs(count: usize) -> AllocReg {
    AllocReg { count }
}

pub struct AllocReg {
    count: usize,
}

impl Emit for AllocReg {
    type Output = usize;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        Ok(program.alloc_registers(self.count))
    }
}

/// Allocate registers and initialize them to NULL.
#[inline(always)]
pub fn alloc_regs_null(count: usize) -> AllocRegsNull {
    AllocRegsNull { count }
}

pub struct AllocRegsNull {
    count: usize,
}

impl Emit for AllocRegsNull {
    type Output = usize;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        Ok(program.alloc_registers_and_init_w_null(self.count))
    }
}

// =============================================================================
// Label Management
// =============================================================================

/// Create a scoped label that will be resolved at the end of the scope.
///
/// This is a higher-order combinator that ensures labels are properly scoped.
/// The label is created, the inner computation runs, and the label is resolved
/// to point to the instruction after the inner computation.
///
/// # Example
/// ```ignore
/// with_forward_label(|end_label| {
///     emit_condition_check(reg, end_label)
///         .and_then(emit_loop_body())
/// })
/// // end_label now points here
/// ```
#[inline(always)]
pub fn with_forward_label<F, E>(f: F) -> WithForwardLabel<F>
where
    F: FnOnce(BranchOffset) -> E,
    E: Emit,
{
    WithForwardLabel { f }
}

pub struct WithForwardLabel<F> {
    f: F,
}

impl<F, E> Emit for WithForwardLabel<F>
where
    F: FnOnce(BranchOffset) -> E,
    E: Emit,
{
    type Output = E::Output;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let label = program.allocate_label();
        let result = (self.f)(label).run(program)?;
        program.resolve_label(label, program.offset());
        Ok(result)
    }
}

/// Allocate a label without resolving it.
///
/// Use this when you need manual control over label resolution.
#[inline(always)]
pub fn alloc_label() -> AllocLabel {
    AllocLabel
}

pub struct AllocLabel;

impl Emit for AllocLabel {
    type Output = BranchOffset;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        Ok(program.allocate_label())
    }
}

/// Allocate multiple labels at once.
///
/// # Example
/// ```ignore
/// alloc_labels(3).then(|labels| {
///     goto(labels[0]).and_then(preassign_label(labels[1]))
/// })
/// ```
#[inline(always)]
pub fn alloc_labels(count: usize) -> AllocLabels {
    AllocLabels { count }
}

pub struct AllocLabels {
    count: usize,
}

impl Emit for AllocLabels {
    type Output = Vec<BranchOffset>;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        Ok((0..self.count).map(|_| program.allocate_label()).collect())
    }
}

/// Resolve a label to the current instruction offset.
#[inline(always)]
pub fn resolve_label(label: BranchOffset) -> ResolveLabel {
    ResolveLabel { label }
}

pub struct ResolveLabel {
    label: BranchOffset,
}

impl Emit for ResolveLabel {
    type Output = ();

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        program.resolve_label(self.label, program.offset());
        Ok(())
    }
}

/// Get the current instruction offset.
#[inline(always)]
pub fn current_offset() -> CurrentOffset {
    CurrentOffset
}

pub struct CurrentOffset;

impl Emit for CurrentOffset {
    type Output = BranchOffset;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        Ok(program.offset())
    }
}

// =============================================================================
// Instruction Emission Primitives
// =============================================================================

/// Emit a single instruction.
///
/// This is the fundamental emission primitive.
///
/// # Example
/// ```ignore
/// insn(Insn::Integer { value: 42, dest: reg })
/// ```
#[inline(always)]
pub fn insn(instruction: Insn) -> EmitInsn {
    EmitInsn { instruction }
}

pub struct EmitInsn {
    instruction: Insn,
}

impl Emit for EmitInsn {
    type Output = ();

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        program.emit_insn(self.instruction);
        Ok(())
    }
}

/// Emit an integer constant into a register.
#[inline(always)]
pub fn integer(value: i64, dest: usize) -> EmitInsn {
    insn(Insn::Integer { value, dest })
}

/// Emit an integer and return the destination register.
///
/// Combines allocation and emission for convenience.
#[inline(always)]
pub fn integer_new_reg(value: i64) -> impl Emit<Output = usize> {
    alloc_reg().then(move |reg| integer(value, reg).map(move |_| reg))
}

/// Emit a NULL into a register.
#[inline(always)]
pub fn null(dest: usize) -> EmitInsn {
    insn(Insn::Null {
        dest,
        dest_end: None,
    })
}

/// Emit NULL into a range of registers.
#[inline(always)]
pub fn null_range(dest: usize, dest_end: usize) -> EmitInsn {
    insn(Insn::Null {
        dest,
        dest_end: Some(dest_end),
    })
}

/// Emit a string constant into a register.
#[inline(always)]
pub fn string8(value: String, dest: usize) -> EmitInsn {
    insn(Insn::String8 { value, dest })
}

/// Emit an unconditional jump.
#[inline(always)]
pub fn goto(target_pc: BranchOffset) -> EmitInsn {
    insn(Insn::Goto { target_pc })
}

/// Emit a conditional jump if register is true.
#[inline(always)]
pub fn if_true(reg: usize, target_pc: BranchOffset, jump_if_null: bool) -> EmitInsn {
    insn(Insn::If {
        reg,
        target_pc,
        jump_if_null,
    })
}

/// Emit a conditional jump if register is false.
#[inline(always)]
pub fn if_not(reg: usize, target_pc: BranchOffset, jump_if_null: bool) -> EmitInsn {
    insn(Insn::IfNot {
        reg,
        target_pc,
        jump_if_null,
    })
}

/// Emit a result row.
#[inline(always)]
pub fn result_row(start_reg: usize, count: usize) -> EmitInsn {
    insn(Insn::ResultRow { start_reg, count })
}

/// Emit a halt instruction.
#[inline(always)]
pub fn halt() -> EmitInsn {
    insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    })
}

/// Emit a copy instruction.
#[inline(always)]
pub fn copy(source: usize, dest: usize) -> EmitInsn {
    insn(Insn::Copy {
        src_reg: source,
        dst_reg: dest,
        extra_amount: 0, // 0 means copy just the one register
    })
}

// =============================================================================
// Arithmetic Operations
// =============================================================================

/// Emit an addition instruction.
#[inline(always)]
pub fn add(lhs: usize, rhs: usize, dest: usize) -> EmitInsn {
    insn(Insn::Add { lhs, rhs, dest })
}

/// Emit a subtraction instruction.
#[inline(always)]
pub fn subtract(lhs: usize, rhs: usize, dest: usize) -> EmitInsn {
    insn(Insn::Subtract { lhs, rhs, dest })
}

/// Emit a multiplication instruction.
#[inline(always)]
pub fn multiply(lhs: usize, rhs: usize, dest: usize) -> EmitInsn {
    insn(Insn::Multiply { lhs, rhs, dest })
}

/// Emit a division instruction.
#[inline(always)]
pub fn divide(lhs: usize, rhs: usize, dest: usize) -> EmitInsn {
    insn(Insn::Divide { lhs, rhs, dest })
}

// =============================================================================
// Cursor and Column Operations
// =============================================================================

/// Emit a Rewind instruction - position cursor at first row.
/// Returns the "empty" label that should be jumped to when the cursor is empty.
#[inline(always)]
pub fn rewind(cursor_id: usize, pc_if_empty: BranchOffset) -> EmitInsn {
    insn(Insn::Rewind {
        cursor_id,
        pc_if_empty,
    })
}

/// Emit a Next instruction - advance cursor to next row.
#[inline(always)]
pub fn next(cursor_id: usize, pc_if_next: BranchOffset) -> EmitInsn {
    insn(Insn::Next {
        cursor_id,
        pc_if_next,
    })
}

/// Emit a Column instruction - read a column value into a register.
#[inline(always)]
pub fn column(cursor_id: usize, column_idx: usize, dest: usize) -> EmitInsn {
    insn(Insn::Column {
        cursor_id,
        column: column_idx,
        dest,
        default: None,
    })
}

/// Emit a RowId instruction - get the rowid of the current row.
#[inline(always)]
pub fn rowid(cursor_id: usize, dest: usize) -> EmitInsn {
    insn(Insn::RowId { cursor_id, dest })
}

// =============================================================================
// Comparison Operations
// =============================================================================

/// Emit a Ne (not equal) comparison that jumps if not equal.
#[inline(always)]
pub fn ne_jump(
    lhs: usize,
    rhs: usize,
    target_pc: BranchOffset,
    flags: crate::vdbe::insn::CmpInsFlags,
    collation: Option<crate::translate::collate::CollationSeq>,
) -> EmitInsn {
    insn(Insn::Ne {
        lhs,
        rhs,
        target_pc,
        flags,
        collation,
    })
}

/// Emit an IsNull instruction - jump if register is NULL.
#[inline(always)]
pub fn is_null(reg: usize, target_pc: BranchOffset) -> EmitInsn {
    insn(Insn::IsNull { reg, target_pc })
}

// =============================================================================
// Function Calls
// =============================================================================

/// Emit a Function call instruction.
#[inline(always)]
pub fn function_call(
    start_reg: usize,
    dest: usize,
    func: crate::function::FuncCtx,
    constant_mask: i32,
) -> EmitInsn {
    insn(Insn::Function {
        constant_mask,
        start_reg,
        dest,
        func,
    })
}

// =============================================================================
// Record and Insert Operations
// =============================================================================

/// Emit a MakeRecord instruction.
#[inline(always)]
pub fn make_record(start_reg: usize, count: usize, dest_reg: usize) -> EmitInsn {
    insn(Insn::MakeRecord {
        start_reg: crate::vdbe::insn::to_u16(start_reg),
        count: crate::vdbe::insn::to_u16(count),
        dest_reg: crate::vdbe::insn::to_u16(dest_reg),
        index_name: None,
        affinity_str: None,
    })
}

/// Emit a NewRowid instruction.
#[inline(always)]
pub fn new_rowid(cursor: usize, rowid_reg: usize) -> EmitInsn {
    insn(Insn::NewRowid {
        cursor,
        rowid_reg,
        prev_largest_reg: 0,
    })
}

/// Emit an Insert instruction.
#[inline(always)]
pub fn insert(cursor: usize, key_reg: usize, record_reg: usize, table_name: String) -> EmitInsn {
    insn(Insn::Insert {
        cursor,
        key_reg,
        record_reg,
        flag: Default::default(),
        table_name,
    })
}

// =============================================================================
// Label Operations with Preassignment
// =============================================================================

/// Allocate a label and preassign it to the next instruction.
///
/// This is useful when you need a label that points to the *current* position
/// rather than a forward reference.
#[inline(always)]
pub fn preassigned_label() -> PreassignedLabel {
    PreassignedLabel
}

pub struct PreassignedLabel;

impl Emit for PreassignedLabel {
    type Output = BranchOffset;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let label = program.allocate_label();
        program.preassign_label_to_next_insn(label);
        Ok(label)
    }
}

/// Preassign an existing label to the next instruction.
#[inline(always)]
pub fn preassign_label(label: BranchOffset) -> PreassignLabel {
    PreassignLabel { label }
}

pub struct PreassignLabel {
    label: BranchOffset,
}

impl Emit for PreassignLabel {
    type Output = ();

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        program.preassign_label_to_next_insn(self.label);
        Ok(())
    }
}

// =============================================================================
// Collection Combinators
// =============================================================================

/// Sequence a vector of emit computations, collecting the results.
///
/// # Example
/// ```ignore
/// let emits: Vec<_> = columns.iter().map(|c| emit_column(c)).collect();
/// sequence(emits)  // -> Emit<Output = Vec<usize>>
/// ```
#[inline(always)]
pub fn sequence<E>(emits: Vec<E>) -> Sequence<E>
where
    E: Emit,
{
    Sequence { emits }
}

pub struct Sequence<E> {
    emits: Vec<E>,
}

impl<E> Emit for Sequence<E>
where
    E: Emit,
{
    type Output = Vec<E::Output>;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        self.emits.into_iter().map(|e| e.run(program)).collect()
    }
}

/// Run an emit computation for each item in an iterator.
///
/// # Example
/// ```ignore
/// for_each(columns.iter(), |col, idx| {
///     emit_column_read(cursor, idx, reg + idx)
/// })
/// ```
#[inline(always)]
pub fn for_each<I, F, E>(iter: I, f: F) -> ForEach<I, F>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> E,
    E: Emit,
{
    ForEach { iter, f }
}

pub struct ForEach<I, F> {
    iter: I,
    f: F,
}

impl<I, F, E> Emit for ForEach<I, F>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> E,
    E: Emit,
{
    type Output = Vec<E::Output>;

    #[inline(always)]
    fn run(mut self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        self.iter
            .into_iter()
            .map(|item| (self.f)(item).run(program))
            .collect()
    }
}

/// Fold over an iterator, accumulating bytecode emission.
///
/// # Example
/// ```ignore
/// fold(0, columns.iter(), |acc, col| {
///     emit_column(col).map(move |_| acc + 1)
/// })
/// ```
#[inline(always)]
pub fn fold<A, I, F, E>(init: A, iter: I, f: F) -> Fold<A, I, F>
where
    I: IntoIterator,
    F: FnMut(A, I::Item) -> E,
    E: Emit<Output = A>,
{
    Fold { acc: init, iter, f }
}

pub struct Fold<A, I, F> {
    acc: A,
    iter: I,
    f: F,
}

impl<A, I, F, E> Emit for Fold<A, I, F>
where
    I: IntoIterator,
    F: FnMut(A, I::Item) -> E,
    E: Emit<Output = A>,
{
    type Output = A;

    #[inline(always)]
    fn run(mut self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let mut acc = self.acc;
        for item in self.iter {
            acc = (self.f)(acc, item).run(program)?;
        }
        Ok(acc)
    }
}

// =============================================================================
// Conditional Combinators
// =============================================================================

/// Conditional emission based on a boolean.
///
/// # Example
/// ```ignore
/// when(has_limit, || emit_limit_init(limit_reg))
/// ```
#[inline(always)]
pub fn when<F, E>(condition: bool, f: F) -> When<F>
where
    F: FnOnce() -> E,
    E: Emit,
{
    When { condition, f }
}

pub struct When<F> {
    condition: bool,
    f: F,
}

impl<F, E> Emit for When<F>
where
    F: FnOnce() -> E,
    E: Emit,
{
    type Output = Option<E::Output>;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        if self.condition {
            Ok(Some((self.f)().run(program)?))
        } else {
            Ok(None)
        }
    }
}

/// Conditional emission with an else branch.
///
/// # Example
/// ```ignore
/// if_else(
///     is_aggregate,
///     || emit_aggregate_init(),
///     || emit_simple_init(),
/// )
/// ```
#[inline(always)]
pub fn if_else<F1, F2, E1, E2, T>(condition: bool, if_true: F1, if_false: F2) -> IfElse<F1, F2>
where
    F1: FnOnce() -> E1,
    F2: FnOnce() -> E2,
    E1: Emit<Output = T>,
    E2: Emit<Output = T>,
{
    IfElse {
        condition,
        if_true,
        if_false,
    }
}

pub struct IfElse<F1, F2> {
    condition: bool,
    if_true: F1,
    if_false: F2,
}

impl<F1, F2, E1, E2, T> Emit for IfElse<F1, F2>
where
    F1: FnOnce() -> E1,
    F2: FnOnce() -> E2,
    E1: Emit<Output = T>,
    E2: Emit<Output = T>,
{
    type Output = T;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        if self.condition {
            (self.if_true)().run(program)
        } else {
            (self.if_false)().run(program)
        }
    }
}

/// Match on an Option, running different emission paths.
#[inline(always)]
pub fn match_option<T, F1, F2, E1, E2, R>(
    opt: Option<T>,
    some_f: F1,
    none_f: F2,
) -> MatchOption<T, F1, F2>
where
    F1: FnOnce(T) -> E1,
    F2: FnOnce() -> E2,
    E1: Emit<Output = R>,
    E2: Emit<Output = R>,
{
    MatchOption {
        opt,
        some_f,
        none_f,
    }
}

pub struct MatchOption<T, F1, F2> {
    opt: Option<T>,
    some_f: F1,
    none_f: F2,
}

impl<T, F1, F2, E1, E2, R> Emit for MatchOption<T, F1, F2>
where
    F1: FnOnce(T) -> E1,
    F2: FnOnce() -> E2,
    E1: Emit<Output = R>,
    E2: Emit<Output = R>,
{
    type Output = R;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        match self.opt {
            Some(v) => (self.some_f)(v).run(program),
            None => (self.none_f)().run(program),
        }
    }
}

// =============================================================================
// Loop Structure Combinators
// =============================================================================

/// A builder for emitting loop structures.
///
/// This provides a declarative way to emit the common loop pattern:
/// ```text
/// init:
///     <setup code>
/// loop_start:
///     <loop body>
///     Goto loop_start
/// loop_end:
///     <cleanup>
/// ```
#[inline(always)]
pub fn loop_builder<Init, Body, Cleanup>(
    init: Init,
    body: Body,
    cleanup: Cleanup,
) -> LoopBuilder<Init, Body, Cleanup> {
    LoopBuilder {
        init,
        body,
        cleanup,
    }
}

pub struct LoopBuilder<Init, Body, Cleanup> {
    init: Init,
    body: Body,
    cleanup: Cleanup,
}

/// Labels for loop control flow.
#[derive(Clone, Copy)]
pub struct LoopLabels {
    pub start: BranchOffset,
    pub end: BranchOffset,
    pub next: BranchOffset,
}

impl<Init, Body, Cleanup, InitE, BodyE, CleanupE, InitOut, BodyOut, CleanupOut> Emit
    for LoopBuilder<Init, Body, Cleanup>
where
    Init: FnOnce(LoopLabels) -> InitE,
    Body: FnOnce(LoopLabels, InitOut) -> BodyE,
    Cleanup: FnOnce(LoopLabels, BodyOut) -> CleanupE,
    InitE: Emit<Output = InitOut>,
    BodyE: Emit<Output = BodyOut>,
    CleanupE: Emit<Output = CleanupOut>,
{
    type Output = CleanupOut;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let start_label = program.allocate_label();
        let end_label = program.allocate_label();
        let next_label = program.allocate_label();

        let labels = LoopLabels {
            start: start_label,
            end: end_label,
            next: next_label,
        };

        // Run init
        let init_result = (self.init)(labels).run(program)?;

        // Mark loop start
        program.resolve_label(start_label, program.offset());

        // Run body
        let body_result = (self.body)(labels, init_result).run(program)?;

        // Mark next
        program.resolve_label(next_label, program.offset());

        // Emit goto back to start (the body should emit the conditional exit)
        // Note: The body is responsible for emitting the exit condition jump to end_label

        // Mark loop end
        program.resolve_label(end_label, program.offset());

        // Run cleanup
        (self.cleanup)(labels, body_result).run(program)
    }
}

// =============================================================================
// Scoped Resource Management
// =============================================================================

/// Emit bytecode within a constant span context.
///
/// Instructions emitted within this scope may be hoisted for constant optimization.
#[inline(always)]
pub fn constant_span<F, E>(f: F) -> ConstantSpan<F>
where
    F: FnOnce() -> E,
    E: Emit,
{
    ConstantSpan { f }
}

pub struct ConstantSpan<F> {
    f: F,
}

impl<F, E> Emit for ConstantSpan<F>
where
    F: FnOnce() -> E,
    E: Emit,
{
    type Output = E::Output;

    #[inline(always)]
    fn run(self, program: &mut ProgramBuilder) -> Result<Self::Output> {
        let span_idx = program.constant_span_start();
        let result = (self.f)().run(program);
        program.constant_span_end(span_idx);
        result
    }
}

// =============================================================================
// Type-Safe Register Wrappers (Optional Enhancement)
// =============================================================================

/// A typed register that tracks what kind of value it holds.
///
/// This is a zero-cost wrapper that provides type safety at compile time.
/// The `T` parameter is phantom - it doesn't affect runtime representation.
#[derive(Clone, Copy)]
pub struct TypedReg<T> {
    pub reg: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> TypedReg<T> {
    #[inline(always)]
    pub fn new(reg: usize) -> Self {
        TypedReg {
            reg,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline(always)]
    pub fn raw(&self) -> usize {
        self.reg
    }
}

/// Marker types for typed registers
pub mod reg_types {
    pub struct Integer;
    pub struct Text;
    pub struct Blob;
    pub struct Real;
    pub struct Null;
    pub struct Any;
    pub struct Record;
    pub struct Rowid;
}

/// Allocate a typed register.
#[inline(always)]
pub fn alloc_typed_reg<T>() -> impl Emit<Output = TypedReg<T>> {
    alloc_reg().map(TypedReg::new)
}

// =============================================================================
// Higher-Level Bytecode Patterns
// =============================================================================

/// Emit a binary comparison operation.
///
/// This emits:
/// - Evaluate lhs into a register
/// - Evaluate rhs into a register
/// - Compare and jump based on result
#[inline(always)]
pub fn binary_compare<L, R>(
    lhs: L,
    rhs: R,
    target_if_true: BranchOffset,
    cmp_type: CompareType,
) -> impl Emit<Output = ()>
where
    L: Emit<Output = usize>,
    R: Emit<Output = usize>,
{
    lhs.zip(rhs).then(move |(lhs_reg, rhs_reg)| {
        let flags = crate::vdbe::insn::CmpInsFlags::default();
        match cmp_type {
            CompareType::Eq => insn(Insn::Eq {
                lhs: lhs_reg,
                rhs: rhs_reg,
                target_pc: target_if_true,
                flags,
                collation: None,
            }),
            CompareType::Ne => insn(Insn::Ne {
                lhs: lhs_reg,
                rhs: rhs_reg,
                target_pc: target_if_true,
                flags,
                collation: None,
            }),
            CompareType::Lt => insn(Insn::Lt {
                lhs: lhs_reg,
                rhs: rhs_reg,
                target_pc: target_if_true,
                flags,
                collation: None,
            }),
            CompareType::Le => insn(Insn::Le {
                lhs: lhs_reg,
                rhs: rhs_reg,
                target_pc: target_if_true,
                flags,
                collation: None,
            }),
            CompareType::Gt => insn(Insn::Gt {
                lhs: lhs_reg,
                rhs: rhs_reg,
                target_pc: target_if_true,
                flags,
                collation: None,
            }),
            CompareType::Ge => insn(Insn::Ge {
                lhs: lhs_reg,
                rhs: rhs_reg,
                target_pc: target_if_true,
                flags,
                collation: None,
            }),
        }
    })
}

#[derive(Clone, Copy)]
pub enum CompareType {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

// =============================================================================
// Macro for Convenient Emit Creation
// =============================================================================

/// Macro for creating emit blocks with do-notation style.
///
/// # Example
/// ```ignore
/// emit_do! {
///     reg1 <- alloc_reg();
///     _ <- integer(42, reg1);
///     reg2 <- alloc_reg();
///     _ <- add(reg1, reg1, reg2);
///     pure((reg1, reg2))
/// }
/// ```
#[macro_export]
macro_rules! emit_do {
    // Base case: final expression
    ($e:expr) => { $e };

    // Binding case: pattern <- expr; rest
    ($p:pat = $e:expr; $($rest:tt)*) => {
        $e.then(|$p| emit_do!($($rest)*))
    };

    // Discarding case: expr; rest
    ($e:expr; $($rest:tt)*) => {
        $e.and_then(emit_do!($($rest)*))
    };
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a test program builder
    fn test_program() -> ProgramBuilder {
        use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};
        use crate::CaptureDataChangesMode;

        ProgramBuilder::new(
            QueryMode::Normal,
            CaptureDataChangesMode::Off,
            ProgramBuilderOpts {
                num_cursors: 0,
                approx_num_insns: 10,
                approx_num_labels: 5,
            },
        )
    }

    #[test]
    fn test_pure() {
        let mut program = test_program();
        let result = pure(42).run(&mut program).unwrap();
        assert_eq!(result, 42);
        // No instructions should be emitted
        assert_eq!(program.insns.len(), 0);
    }

    #[test]
    fn test_alloc_reg() {
        let mut program = test_program();
        let reg1 = alloc_reg().run(&mut program).unwrap();
        let reg2 = alloc_reg().run(&mut program).unwrap();
        assert_eq!(reg1, 1);
        assert_eq!(reg2, 2);
    }

    #[test]
    fn test_then_composition() {
        let mut program = test_program();

        let computation = alloc_reg().then(|reg| integer(42, reg).map(move |_| reg));

        let reg = computation.run(&mut program).unwrap();
        assert_eq!(reg, 1);
        assert_eq!(program.insns.len(), 1);
    }

    #[test]
    fn test_sequence() {
        let mut program = test_program();

        let computation = sequence(vec![
            integer_new_reg(1),
            integer_new_reg(2),
            integer_new_reg(3),
        ]);

        let regs = computation.run(&mut program).unwrap();
        assert_eq!(regs.len(), 3);
        assert_eq!(program.insns.len(), 3);
    }

    #[test]
    fn test_with_forward_label() {
        let mut program = test_program();

        let computation = with_forward_label(|end_label| {
            alloc_reg().then(move |reg| {
                integer(1, reg)
                    .and_then(if_true(reg, end_label, false))
                    .and_then(integer(2, reg))
            })
        });

        computation.run(&mut program).unwrap();
        // Should have: Integer, If, Integer
        assert_eq!(program.insns.len(), 3);
    }

    #[test]
    fn test_conditional_when() {
        let mut program = test_program();

        // With condition true
        let result = when(true, || integer_new_reg(42))
            .run(&mut program)
            .unwrap();
        assert!(result.is_some());

        // With condition false
        let mut program2 = test_program();
        let result = when(false, || integer_new_reg(42))
            .run(&mut program2)
            .unwrap();
        assert!(result.is_none());
        assert_eq!(program2.insns.len(), 0);
    }
}
