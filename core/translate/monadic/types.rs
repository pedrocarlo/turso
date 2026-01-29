//! Core types for the monadic bytecode emitter.
//!
//! This module defines:
//! - The `Emit<T>` monad for composable bytecode generation
//! - Typed resource references (`Reg`, `Cursor`, `Label`, etc.)
//! - Environment and state types

// Note: This module provides the core types for the monadic emitter API.
// Many types and methods may not be used internally yet but are provided for consumers.
#![allow(dead_code)]

use crate::error::LimboError;
use crate::schema::Schema;
use crate::vdbe::BranchOffset;
use crate::Connection;
use crate::Result;
use crate::SymbolTable;

use super::insn::InsnSpec;

// =============================================================================
// Typed Resource References
// =============================================================================

/// A typed reference to an allocated register.
///
/// Using a newtype wrapper prevents accidentally mixing up register indices
/// with cursor IDs or other numeric values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Reg(pub(crate) usize);

impl Reg {
    /// Get the underlying register index.
    #[inline]
    pub fn index(&self) -> usize {
        self.0
    }
}

impl From<Reg> for usize {
    fn from(reg: Reg) -> usize {
        reg.0
    }
}

/// A contiguous range of registers.
///
/// Used when allocating multiple registers at once, such as for
/// result columns or function arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegRange {
    pub(crate) start: usize,
    pub(crate) count: usize,
}

impl RegRange {
    /// Get the starting register index.
    #[inline]
    pub fn start(&self) -> usize {
        self.start
    }

    /// Get the number of registers in the range.
    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get the register at a specific offset within the range.
    ///
    /// # Panics
    /// Panics if offset >= count.
    #[inline]
    pub fn get(&self, offset: usize) -> Reg {
        assert!(
            offset < self.count,
            "Register offset {offset} out of bounds (count: {})",
            self.count
        );
        Reg(self.start + offset)
    }

    /// Get the first register in the range.
    #[inline]
    pub fn first(&self) -> Reg {
        Reg(self.start)
    }

    /// Iterate over all registers in the range.
    pub fn iter(&self) -> impl Iterator<Item = Reg> {
        (self.start..self.start + self.count).map(Reg)
    }

    /// Check if the range is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// A typed reference to an allocated cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cursor(pub(crate) usize);

impl Cursor {
    /// Get the underlying cursor ID.
    #[inline]
    pub fn id(&self) -> usize {
        self.0
    }
}

impl From<Cursor> for usize {
    fn from(cursor: Cursor) -> usize {
        cursor.0
    }
}

/// A typed reference to a label (jump target).
///
/// Labels support forward references - they can be used in jump
/// instructions before their target position is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Label(pub(crate) u32);

impl Label {
    /// Get the underlying label number.
    #[inline]
    pub fn number(&self) -> u32 {
        self.0
    }

    /// Convert to a BranchOffset for use in instructions.
    #[inline]
    pub fn to_branch_offset(self) -> BranchOffset {
        BranchOffset::Label(self.0)
    }
}

/// A typed reference to a hash table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HashTableId(pub(crate) usize);

/// Base offset for hash table IDs to avoid collision with cursor IDs.
pub(crate) const HASH_TABLE_ID_BASE: usize = 1 << 30;

impl HashTableId {
    /// Get the underlying hash table ID.
    #[inline]
    pub fn id(&self) -> usize {
        self.0
    }
}

/// Instruction position in the instruction buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsnPos(pub(crate) usize);

impl InsnPos {
    /// Get the underlying position.
    #[inline]
    pub fn offset(&self) -> usize {
        self.0
    }
}

// =============================================================================
// Loop Labels Structure
// =============================================================================

/// A complete set of labels for a loop structure.
///
/// Every loop in the bytecode has three key positions:
/// - `start`: Beginning of loop body (target of Next/Prev)
/// - `next`: Position of the Next/Prev instruction
/// - `end`: First instruction after the loop (target when loop is done)
#[derive(Debug, Clone, Copy)]
pub struct LoopLabels {
    /// Jump back to start of loop body.
    pub start: Label,
    /// Position of the iteration instruction (Next/Prev).
    pub next: Label,
    /// Jump to exit the loop.
    pub end: Label,
}

// =============================================================================
// Cursor Metadata
// =============================================================================

/// Describes what kind of cursor is being allocated.
#[derive(Debug, Clone)]
pub enum CursorKind {
    /// A B-tree table cursor.
    BTreeTable {
        root_page: usize,
        table_name: String,
    },
    /// A B-tree index cursor.
    BTreeIndex {
        root_page: usize,
        index_name: String,
    },
    /// A pseudo cursor for reading from a register.
    Pseudo {
        content_reg: Reg,
        num_columns: usize,
    },
    /// A sorter cursor for ORDER BY.
    Sorter { num_columns: usize },
    /// An ephemeral table cursor.
    Ephemeral { is_table: bool },
    /// A virtual table cursor.
    VirtualTable { module_name: String },
}

// =============================================================================
// Label State
// =============================================================================

/// Tracks the resolution state of a label.
#[derive(Debug, Clone)]
pub(crate) enum LabelState {
    /// Label allocated but not yet bound to an instruction position.
    Unresolved,
    /// Label bound to a specific instruction position.
    Resolved(InsnPos),
}

/// Table tracking all allocated labels and their resolution state.
#[derive(Debug, Default)]
pub struct LabelTable {
    labels: Vec<LabelState>,
}

impl LabelTable {
    /// Create a new empty label table.
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    /// Allocate a new label, returning its reference.
    pub fn allocate(&mut self) -> Label {
        let label = Label(self.labels.len() as u32);
        self.labels.push(LabelState::Unresolved);
        label
    }

    /// Resolve a label to an instruction position.
    pub fn resolve(&mut self, label: Label, pos: InsnPos) -> Result<()> {
        let idx = label.0 as usize;
        if idx >= self.labels.len() {
            return Err(LimboError::InternalError(format!(
                "Label {} not allocated",
                label.0
            )));
        }
        match &self.labels[idx] {
            LabelState::Resolved(_) => {
                return Err(LimboError::InternalError(format!(
                    "Label {} already resolved",
                    label.0
                )));
            }
            LabelState::Unresolved => {
                self.labels[idx] = LabelState::Resolved(pos);
            }
        }
        Ok(())
    }

    /// Get the resolved position for a label, if resolved.
    pub fn get_resolved(&self, label: Label) -> Option<InsnPos> {
        let idx = label.0 as usize;
        self.labels.get(idx).and_then(|state| match state {
            LabelState::Resolved(pos) => Some(*pos),
            LabelState::Unresolved => None,
        })
    }

    /// Check if all labels have been resolved.
    pub fn all_resolved(&self) -> bool {
        self.labels
            .iter()
            .all(|state| matches!(state, LabelState::Resolved(_)))
    }

    /// Get the count of unresolved labels.
    pub fn unresolved_count(&self) -> usize {
        self.labels
            .iter()
            .filter(|state| matches!(state, LabelState::Unresolved))
            .count()
    }
}

// =============================================================================
// Cursor Table
// =============================================================================

/// Table tracking all allocated cursors and their metadata.
#[derive(Debug, Default)]
pub struct CursorTable {
    cursors: Vec<Option<CursorKind>>,
}

impl CursorTable {
    /// Create a new empty cursor table.
    pub fn new() -> Self {
        Self {
            cursors: Vec::new(),
        }
    }

    /// Register a cursor with its metadata.
    pub fn register(&mut self, cursor: Cursor, kind: CursorKind) {
        let idx = cursor.0;
        if idx >= self.cursors.len() {
            self.cursors.resize(idx + 1, None);
        }
        self.cursors[idx] = Some(kind);
    }

    /// Get the metadata for a cursor.
    pub fn get(&self, cursor: Cursor) -> Option<&CursorKind> {
        self.cursors.get(cursor.0).and_then(|k| k.as_ref())
    }
}

// =============================================================================
// Emit Environment (Immutable)
// =============================================================================

/// Immutable environment available to all emission computations.
///
/// This contains references to schema information and other read-only
/// context needed during bytecode generation.
pub struct EmitEnv<'a> {
    /// Database schema for table/index lookups.
    pub schema: &'a Schema,
    /// Symbol table for function resolution.
    pub symbol_table: &'a SymbolTable,
    /// Connection reference for runtime features.
    pub connection: &'a Connection,
}

impl<'a> EmitEnv<'a> {
    /// Create a new emission environment.
    pub fn new(
        schema: &'a Schema,
        symbol_table: &'a SymbolTable,
        connection: &'a Connection,
    ) -> Self {
        Self {
            schema,
            symbol_table,
            connection,
        }
    }
}

// =============================================================================
// Emit State (Mutable)
// =============================================================================

/// Mutable state threaded through emission computations.
///
/// This contains all the state that changes during bytecode generation:
/// - Resource allocation counters
/// - Instruction buffer
/// - Label resolution table
pub struct EmitState {
    /// Next available register index.
    pub(crate) next_register: usize,
    /// Next available cursor ID.
    pub(crate) next_cursor: usize,
    /// Next available label number.
    pub(crate) next_label: u32,
    /// Next available hash table ID (offset from HASH_TABLE_ID_BASE).
    pub(crate) next_hash_table: usize,
    /// Instruction buffer (append-only).
    pub(crate) instructions: Vec<InsnSpec>,
    /// Label resolution table.
    pub(crate) labels: LabelTable,
    /// Cursor metadata table.
    pub(crate) cursors: CursorTable,
    /// Current nesting depth (for subqueries).
    pub(crate) nesting_depth: usize,
}

impl EmitState {
    /// Create a new emission state with default initial values.
    pub fn new() -> Self {
        Self {
            next_register: 1, // Register 0 is often reserved
            next_cursor: 0,
            next_label: 0,
            next_hash_table: 0,
            instructions: Vec::new(),
            labels: LabelTable::new(),
            cursors: CursorTable::new(),
            nesting_depth: 0,
        }
    }

    /// Create emission state starting from specific resource counters.
    ///
    /// Useful when integrating with existing imperative code.
    pub fn with_counters(
        next_register: usize,
        next_cursor: usize,
        next_label: u32,
        next_hash_table: usize,
    ) -> Self {
        Self {
            next_register,
            next_cursor,
            next_label,
            next_hash_table,
            instructions: Vec::new(),
            labels: LabelTable::new(),
            cursors: CursorTable::new(),
            nesting_depth: 0,
        }
    }

    /// Get the current instruction count.
    pub fn instruction_count(&self) -> usize {
        self.instructions.len()
    }

    /// Get a reference to the instruction buffer.
    pub fn instructions(&self) -> &[InsnSpec] {
        &self.instructions
    }

    /// Get the current nesting depth.
    pub fn nesting_depth(&self) -> usize {
        self.nesting_depth
    }
}

impl Default for EmitState {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// The Emit Monad
// =============================================================================

/// A computation in the emission context that produces a value of type `T`.
///
/// `Emit<'a, T>` is a **Reader + State + Error** monad stack:
/// - **Reader**: Access to immutable `EmitEnv` (schema, symbol table, connection)
/// - **State**: Threads mutable `EmitState` (registers, cursors, labels, instructions)
/// - **Error**: Short-circuits on `LimboError`
///
/// # Example
///
/// ```ignore
/// // Allocate a register and load an integer into it
/// let load_42: Emit<'_, Reg> = alloc_reg().flat_map(|reg| {
///     emit(InsnSpec::Integer { value: 42, dest: reg }).map(move |_| reg)
/// });
/// ```
pub struct Emit<'a, T> {
    /// The computation to run.
    #[allow(clippy::type_complexity)]
    run: Box<dyn FnOnce(&EmitEnv<'a>, &mut EmitState) -> Result<T> + 'a>,
}

impl<'a, T: 'a> Emit<'a, T> {
    // -------------------------------------------------------------------------
    // Core Constructors
    // -------------------------------------------------------------------------

    /// Create a computation from a function.
    pub fn new<F>(f: F) -> Self
    where
        F: FnOnce(&EmitEnv<'a>, &mut EmitState) -> Result<T> + 'a,
    {
        Emit { run: Box::new(f) }
    }

    /// Create a pure computation that produces a value without side effects.
    pub fn pure(value: T) -> Self
    where
        T: 'a,
    {
        Emit::new(move |_, _| Ok(value))
    }

    /// Create a computation that always fails with the given error.
    pub fn fail(error: LimboError) -> Self {
        Emit::new(move |_, _| Err(error))
    }

    /// Execute the computation with the given environment and state.
    pub fn run(self, env: &EmitEnv<'a>, state: &mut EmitState) -> Result<T> {
        (self.run)(env, state)
    }

    // -------------------------------------------------------------------------
    // Functor Operations
    // -------------------------------------------------------------------------

    /// Transform the result using a function (Functor::map).
    pub fn map<U, F>(self, f: F) -> Emit<'a, U>
    where
        F: FnOnce(T) -> U + 'a,
        U: 'a,
    {
        Emit::new(move |env, state| {
            let t = self.run(env, state)?;
            Ok(f(t))
        })
    }

    /// Replace the result with a constant value.
    pub fn map_to<U>(self, value: U) -> Emit<'a, U>
    where
        U: 'a,
    {
        self.map(move |_| value)
    }

    /// Discard the result, keeping only the side effects.
    pub fn void(self) -> Emit<'a, ()> {
        self.map(|_| ())
    }

    // -------------------------------------------------------------------------
    // Monad Operations
    // -------------------------------------------------------------------------

    /// Chain computations (Monad::flat_map / bind / >>=).
    ///
    /// The result of the first computation is passed to `f`, which
    /// produces the next computation to run.
    pub fn flat_map<U, F>(self, f: F) -> Emit<'a, U>
    where
        F: FnOnce(T) -> Emit<'a, U> + 'a,
        U: 'a,
    {
        Emit::new(move |env, state| {
            let t = self.run(env, state)?;
            f(t).run(env, state)
        })
    }

    /// Sequence two computations, discarding the first result.
    pub fn then<U>(self, next: Emit<'a, U>) -> Emit<'a, U>
    where
        U: 'a,
    {
        self.flat_map(move |_| next)
    }

    /// Sequence two computations, keeping the first result.
    pub fn before<U>(self, next: Emit<'a, U>) -> Emit<'a, T>
    where
        U: 'a,
    {
        self.flat_map(move |t| next.map(move |_| t))
    }

    // -------------------------------------------------------------------------
    // Applicative Operations
    // -------------------------------------------------------------------------

    /// Combine with another computation, keeping both results.
    pub fn zip<U>(self, other: Emit<'a, U>) -> Emit<'a, (T, U)>
    where
        U: 'a,
    {
        self.flat_map(move |t| other.map(move |u| (t, u)))
    }

    /// Combine three computations.
    pub fn zip3<U, V>(self, second: Emit<'a, U>, third: Emit<'a, V>) -> Emit<'a, (T, U, V)>
    where
        U: 'a,
        V: 'a,
    {
        self.flat_map(move |t| second.flat_map(move |u| third.map(move |v| (t, u, v))))
    }

    /// Lift a binary function to work on Emit values.
    pub fn lift2<U, V, F>(f: F, ea: Emit<'a, T>, eb: Emit<'a, U>) -> Emit<'a, V>
    where
        F: FnOnce(T, U) -> V + 'a,
        U: 'a,
        V: 'a,
    {
        ea.flat_map(move |a| eb.map(move |b| f(a, b)))
    }

    // -------------------------------------------------------------------------
    // Error Handling
    // -------------------------------------------------------------------------

    /// Provide a fallback computation on error.
    pub fn or_else<F>(self, f: F) -> Self
    where
        F: FnOnce(LimboError) -> Self + 'a,
    {
        Emit::new(move |env, state| match self.run(env, state) {
            Ok(t) => Ok(t),
            Err(e) => f(e).run(env, state),
        })
    }

    /// Provide a default value on error.
    pub fn or(self, default: T) -> Self
    where
        T: 'a,
    {
        self.or_else(move |_| Emit::pure(default))
    }

    /// Transform errors using a function.
    pub fn map_err<F>(self, f: F) -> Self
    where
        F: FnOnce(LimboError) -> LimboError + 'a,
    {
        Emit::new(move |env, state| self.run(env, state).map_err(f))
    }

    // -------------------------------------------------------------------------
    // Conditional Execution
    // -------------------------------------------------------------------------

    /// Run computation only if condition is true, returning `Some(result)`.
    /// Returns `None` if condition is false.
    pub fn when(condition: bool, computation: Self) -> Emit<'a, Option<T>> {
        if condition {
            computation.map(Some)
        } else {
            Emit::pure(None)
        }
    }

    /// Run computation only if condition is true, returning default if false.
    pub fn when_or(condition: bool, computation: Self, default: T) -> Self
    where
        T: 'a,
    {
        if condition {
            computation
        } else {
            Emit::pure(default)
        }
    }
}

// =============================================================================
// Utility Functions
// =============================================================================

// Note: These functions are part of the public monadic emitter API.
// They may not be used internally yet, but are provided for consumers.

/// Sequence a vector of computations into a computation of a vector.
///
/// Computations are executed in order, and if any fails, the entire
/// sequence fails.
#[allow(dead_code)]
pub fn sequence<'a, T: 'a>(computations: Vec<Emit<'a, T>>) -> Emit<'a, Vec<T>> {
    computations
        .into_iter()
        .fold(Emit::pure(Vec::new()), |acc, emit| {
            acc.flat_map(move |mut vec| {
                emit.map(move |t| {
                    vec.push(t);
                    vec
                })
            })
        })
}

/// Traverse a collection, applying a computation to each element.
///
/// This is like `map` followed by `sequence`.
#[allow(dead_code)]
pub fn traverse<'a, T, U: 'a, F>(items: Vec<T>, f: F) -> Emit<'a, Vec<U>>
where
    F: Fn(T) -> Emit<'a, U> + 'a,
    T: 'a,
{
    sequence(items.into_iter().map(f).collect())
}

/// Run a computation for each item, discarding results.
#[allow(dead_code)]
pub fn for_each_item<'a, T, F>(items: Vec<T>, f: F) -> Emit<'a, ()>
where
    F: Fn(T) -> Emit<'a, ()> + 'a,
    T: 'a,
{
    traverse(items, f).void()
}

/// Choose between two computations based on a condition.
#[allow(dead_code)]
pub fn if_then_else<'a, T: 'a>(
    condition: bool,
    then_branch: Emit<'a, T>,
    else_branch: Emit<'a, T>,
) -> Emit<'a, T> {
    if condition {
        then_branch
    } else {
        else_branch
    }
}

/// Pattern match on an Option, running different computations for Some/None.
#[allow(dead_code)]
pub fn match_option<'a, T, U: 'a>(
    opt: Option<T>,
    some_branch: impl FnOnce(T) -> Emit<'a, U> + 'a,
    none_branch: Emit<'a, U>,
) -> Emit<'a, U>
where
    T: 'a,
{
    match opt {
        Some(t) => some_branch(t),
        None => none_branch,
    }
}

/// Get a value from the environment.
#[allow(dead_code)]
pub fn ask<'a, T, F>(f: F) -> Emit<'a, T>
where
    F: FnOnce(&EmitEnv<'a>) -> T + 'a,
    T: 'a,
{
    Emit::new(move |env, _| Ok(f(env)))
}

/// Get a value from the state.
#[allow(dead_code)]
pub fn get<'a, T, F>(f: F) -> Emit<'a, T>
where
    F: FnOnce(&EmitState) -> T + 'a,
    T: 'a,
{
    Emit::new(move |_, state| Ok(f(state)))
}

/// Modify the state.
#[allow(dead_code)]
pub fn modify<'a, F>(f: F) -> Emit<'a, ()>
where
    F: FnOnce(&mut EmitState) + 'a,
{
    Emit::new(move |_, state| {
        f(state);
        Ok(())
    })
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;

    /// A test environment that can be used for unit tests.
    ///
    /// This creates a minimal environment with empty Schema and SymbolTable.
    /// The Connection field is set to a dummy value using Box::leak.
    /// This is safe for tests as the memory is never freed but tests are short-lived.
    pub struct TestEnv {
        schema: Schema,
        syms: SymbolTable,
    }

    impl TestEnv {
        pub fn new() -> Self {
            Self {
                schema: Schema::new(),
                syms: SymbolTable::new(),
            }
        }

        /// Run an Emit computation in this test environment.
        ///
        /// Note: This creates a dummy Connection pointer internally. The computation
        /// must not actually dereference the connection - this is only for tests
        /// that don't need real database access.
        pub fn run<'a, T: 'a>(&'a self, computation: Emit<'a, T>) -> crate::Result<(T, EmitState)> {
            // Create a dummy Connection pointer. This is unsafe but acceptable for tests
            // that don't actually use the connection. For tests that need real DB access,
            // use integration tests with proper Connection setup.
            let dummy_conn: &Connection = unsafe {
                // We create a properly aligned pointer that will never be dereferenced
                std::ptr::NonNull::dangling().as_ref()
            };

            let env = EmitEnv {
                schema: &self.schema,
                symbol_table: &self.syms,
                connection: dummy_conn,
            };

            let mut state = EmitState::new();
            let result = computation.run(&env, &mut state)?;
            Ok((result, state))
        }
    }

    impl Default for TestEnv {
        fn default() -> Self {
            Self::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers::TestEnv;
    use super::*;

    #[test]
    fn test_pure() {
        let env = TestEnv::new();
        let (result, _state) = env.run(Emit::pure(42)).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_map() {
        let env = TestEnv::new();
        let computation = Emit::pure(21).map(|x| x * 2);
        let (result, _state) = env.run(computation).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_reg_range() {
        let range = RegRange { start: 5, count: 3 };
        assert_eq!(range.start(), 5);
        assert_eq!(range.count(), 3);
        assert_eq!(range.get(0).index(), 5);
        assert_eq!(range.get(1).index(), 6);
        assert_eq!(range.get(2).index(), 7);

        let regs: Vec<_> = range.iter().collect();
        assert_eq!(regs.len(), 3);
        assert_eq!(regs[0].index(), 5);
        assert_eq!(regs[2].index(), 7);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn test_reg_range_bounds() {
        let range = RegRange { start: 5, count: 3 };
        range.get(3); // Should panic
    }

    #[test]
    fn test_label_table() {
        let mut table = LabelTable::new();

        let label1 = table.allocate();
        let label2 = table.allocate();

        assert_eq!(label1.number(), 0);
        assert_eq!(label2.number(), 1);
        assert!(!table.all_resolved());
        assert_eq!(table.unresolved_count(), 2);

        table.resolve(label1, InsnPos(10)).unwrap();
        assert_eq!(table.get_resolved(label1), Some(InsnPos(10)));
        assert_eq!(table.get_resolved(label2), None);
        assert_eq!(table.unresolved_count(), 1);

        table.resolve(label2, InsnPos(20)).unwrap();
        assert!(table.all_resolved());
    }

    #[test]
    fn test_label_double_resolve_error() {
        let mut table = LabelTable::new();
        let label = table.allocate();

        table.resolve(label, InsnPos(10)).unwrap();
        let result = table.resolve(label, InsnPos(20));
        assert!(result.is_err());
    }
}
