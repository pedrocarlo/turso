# Monadic Emitter Specification

## Overview

This specification defines a monadic composition system for the Turso translate emitter that replaces imperative state threading with declarative, composable computations. The design addresses:

1. Resource allocation (registers, cursors, labels, hash tables)
2. Label backpatching (forward references resolved later)
3. Instruction emission as a pure computation
4. Error short-circuiting with partial state preservation
5. Conditional execution paths based on query plan
6. Nested contexts for subqueries and coroutines
7. Type-safe state threading through computation chains

---

## Part 1: Core Monad Definition

### 1.1 The Emit Monad

The `Emit<T>` monad represents a computation that:
- Reads from an immutable environment (`EmitEnv`)
- Threads mutable state (`EmitState`)
- May fail with an error (`LimboError`)
- Produces a value of type `T`

```rust
/// A computation in the emission context that produces a value of type T.
///
/// This is a Reader + State + Error monad stack:
/// - Reader: access to immutable query plan and schema
/// - State: mutable allocation counters and instruction buffer
/// - Error: short-circuiting on failures
pub struct Emit<'a, T> {
    run: Box<dyn FnOnce(&EmitEnv<'a>, &mut EmitState) -> Result<T> + 'a>,
}

/// Immutable environment available to all computations
pub struct EmitEnv<'a> {
    pub schema: &'a Schema,
    pub symbol_table: &'a SymbolTable,
    pub connection: &'a Arc<Connection>,
}

/// Mutable state threaded through computations
pub struct EmitState {
    // Resource counters (monotonically increasing)
    next_register: usize,
    next_cursor: usize,
    next_label: usize,
    next_hash_table: usize,

    // Instruction buffer (append-only)
    instructions: Vec<InsnSpec>,

    // Label resolution table (forward references)
    labels: LabelTable,

    // Cursor metadata
    cursors: CursorTable,

    // Accumulated metadata (set once, read many)
    metadata: EmitMetadata,

    // Nesting depth for subqueries
    nesting_depth: usize,
}
```

### 1.2 Monad Implementation

```rust
impl<'a, T: 'a> Emit<'a, T> {
    /// Create a pure computation that produces a value
    pub fn pure(value: T) -> Self
    where
        T: Clone
    {
        Emit {
            run: Box::new(move |_, _| Ok(value.clone())),
        }
    }

    /// Create a computation from a function
    pub fn new<F>(f: F) -> Self
    where
        F: FnOnce(&EmitEnv<'a>, &mut EmitState) -> Result<T> + 'a,
    {
        Emit { run: Box::new(f) }
    }

    /// Execute the computation
    pub fn run(self, env: &EmitEnv<'a>, state: &mut EmitState) -> Result<T> {
        (self.run)(env, state)
    }

    /// Transform the result (Functor::map)
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

    /// Chain computations (Monad::flat_map / bind)
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

    /// Combine with another computation, keeping both results
    pub fn zip<U>(self, other: Emit<'a, U>) -> Emit<'a, (T, U)>
    where
        U: 'a,
    {
        self.flat_map(move |t| other.map(move |u| (t, u)))
    }

    /// Run computation only if condition is true
    pub fn when(condition: bool, computation: Self) -> Emit<'a, Option<T>> {
        if condition {
            computation.map(Some)
        } else {
            Emit::pure(None)
        }
    }

    /// Provide a default on error
    pub fn or_else<F>(self, f: F) -> Self
    where
        F: FnOnce(LimboError) -> Self + 'a,
    {
        Emit::new(move |env, state| {
            match self.run(env, state) {
                Ok(t) => Ok(t),
                Err(e) => f(e).run(env, state),
            }
        })
    }
}
```

### 1.3 Applicative Operations

```rust
impl<'a, T: 'a> Emit<'a, T> {
    /// Apply a function inside Emit to a value inside Emit
    pub fn ap<U, F>(ef: Emit<'a, F>, ea: Emit<'a, T>) -> Emit<'a, U>
    where
        F: FnOnce(T) -> U + 'a,
        U: 'a,
    {
        ef.flat_map(move |f| ea.map(f))
    }

    /// Lift a binary function to work on Emit values
    pub fn lift2<U, V, F>(f: F, ea: Emit<'a, T>, eb: Emit<'a, U>) -> Emit<'a, V>
    where
        F: FnOnce(T, U) -> V + 'a,
        U: 'a,
        V: 'a,
    {
        ea.flat_map(move |a| eb.map(move |b| f(a, b)))
    }
}

/// Sequence a vector of computations into a computation of a vector
pub fn sequence<'a, T: 'a>(computations: Vec<Emit<'a, T>>) -> Emit<'a, Vec<T>> {
    computations.into_iter().fold(
        Emit::pure(Vec::new()),
        |acc, emit| {
            acc.flat_map(move |mut vec| {
                emit.map(move |t| {
                    vec.push(t);
                    vec
                })
            })
        },
    )
}

/// Traverse a collection, applying a computation to each element
pub fn traverse<'a, T, U: 'a, F>(items: Vec<T>, f: F) -> Emit<'a, Vec<U>>
where
    F: Fn(T) -> Emit<'a, U>,
{
    sequence(items.into_iter().map(f).collect())
}
```

---

## Part 2: Resource Allocation Primitives

### 2.1 Register Allocation

```rust
/// A typed reference to an allocated register
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Reg(usize);

impl Reg {
    pub fn index(&self) -> usize { self.0 }
}

/// A contiguous range of registers
#[derive(Debug, Clone, Copy)]
pub struct RegRange {
    start: usize,
    count: usize,
}

impl RegRange {
    pub fn start(&self) -> usize { self.start }
    pub fn count(&self) -> usize { self.count }
    pub fn get(&self, offset: usize) -> Reg {
        assert!(offset < self.count, "Register offset out of bounds");
        Reg(self.start + offset)
    }
    pub fn iter(&self) -> impl Iterator<Item = Reg> {
        (self.start..self.start + self.count).map(Reg)
    }
}

/// Allocate a single register
pub fn alloc_reg<'a>() -> Emit<'a, Reg> {
    Emit::new(|_, state| {
        let reg = Reg(state.next_register);
        state.next_register += 1;
        Ok(reg)
    })
}

/// Allocate a contiguous range of registers
pub fn alloc_regs<'a>(count: usize) -> Emit<'a, RegRange> {
    Emit::new(move |_, state| {
        let start = state.next_register;
        state.next_register += count;
        Ok(RegRange { start, count })
    })
}

/// Allocate and initialize a register with null
pub fn alloc_reg_null<'a>() -> Emit<'a, Reg> {
    alloc_reg().flat_map(|reg| {
        emit(InsnSpec::Null { dest: reg, count: 1 }).map(move |_| reg)
    })
}

/// Allocate and initialize a range with nulls
pub fn alloc_regs_null<'a>(count: usize) -> Emit<'a, RegRange> {
    alloc_regs(count).flat_map(move |range| {
        emit(InsnSpec::Null { dest: Reg(range.start), count }).map(move |_| range)
    })
}

/// Allocate and initialize a register with an integer
pub fn alloc_reg_int<'a>(value: i64) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |reg| {
        emit(InsnSpec::Integer { value, dest: reg }).map(move |_| reg)
    })
}
```

### 2.2 Cursor Allocation

```rust
/// A typed reference to an allocated cursor
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cursor(usize);

impl Cursor {
    pub fn id(&self) -> usize { self.0 }
}

/// Cursor types for metadata tracking
#[derive(Debug, Clone)]
pub enum CursorKind {
    BTreeTable { root_page: usize, table_name: String },
    BTreeIndex { root_page: usize, index_name: String },
    Pseudo,
    Sorter { columns: usize },
    Ephemeral { is_table: bool },
    VirtualTable { module: String },
}

/// Allocate a cursor with metadata
pub fn alloc_cursor<'a>(kind: CursorKind) -> Emit<'a, Cursor> {
    Emit::new(move |_, state| {
        let cursor = Cursor(state.next_cursor);
        state.next_cursor += 1;
        state.cursors.register(cursor, kind);
        Ok(cursor)
    })
}

/// Allocate a cursor for a table
pub fn alloc_table_cursor<'a>(table: &Table) -> Emit<'a, Cursor> {
    alloc_cursor(CursorKind::BTreeTable {
        root_page: table.root_page,
        table_name: table.name.clone(),
    })
}

/// Allocate a cursor for an index
pub fn alloc_index_cursor<'a>(index: &Index) -> Emit<'a, Cursor> {
    alloc_cursor(CursorKind::BTreeIndex {
        root_page: index.root_page,
        index_name: index.name.clone(),
    })
}

/// Allocate a sorter cursor
pub fn alloc_sorter<'a>(columns: usize) -> Emit<'a, Cursor> {
    alloc_cursor(CursorKind::Sorter { columns })
}

/// Allocate an ephemeral table cursor
pub fn alloc_ephemeral<'a>(is_table: bool) -> Emit<'a, Cursor> {
    alloc_cursor(CursorKind::Ephemeral { is_table })
}
```

### 2.3 Label Allocation and Resolution

Labels are the most complex resource because they require forward references (backpatching).

```rust
/// A typed reference to a label (may be unresolved)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Label(usize);

/// Resolved instruction position
#[derive(Debug, Clone, Copy)]
pub struct InsnPos(usize);

/// Label resolution state
pub struct LabelTable {
    labels: Vec<LabelState>,
}

enum LabelState {
    /// Label allocated but not yet bound to an instruction
    Unresolved {
        forward_refs: Vec<(InsnPos, JumpField)>,
    },
    /// Label bound to a specific instruction position
    Resolved(InsnPos),
}

/// Which field of an instruction holds the jump target
#[derive(Debug, Clone, Copy)]
pub enum JumpField {
    TargetPc,
    PcIfEmpty,
    PcIfFalse,
    PcIfTrue,
    PcIfNext,
    PcIfNull,
    PcIfNotNull,
}

/// Allocate a new label (initially unresolved)
pub fn alloc_label<'a>() -> Emit<'a, Label> {
    Emit::new(|_, state| {
        let label = Label(state.next_label);
        state.next_label += 1;
        state.labels.allocate(label);
        Ok(label)
    })
}

/// Bind a label to the next instruction position
pub fn bind_label<'a>(label: Label) -> Emit<'a, ()> {
    Emit::new(move |_, state| {
        let pos = InsnPos(state.instructions.len());
        state.labels.resolve(label, pos)?;
        Ok(())
    })
}

/// Create a label and immediately bind it to the next position
pub fn here<'a>() -> Emit<'a, Label> {
    alloc_label().flat_map(|label| {
        bind_label(label).map(move |_| label)
    })
}

/// A scoped label that creates forward and backward jump points
pub struct LoopLabels {
    pub start: Label,   // Jump back to loop start
    pub next: Label,    // Jump to next iteration
    pub end: Label,     // Jump to exit loop
}

/// Allocate a complete set of loop labels
pub fn alloc_loop_labels<'a>() -> Emit<'a, LoopLabels> {
    Emit::lift2(
        |start, (next, end)| LoopLabels { start, next, end },
        alloc_label(),
        alloc_label().zip(alloc_label()),
    )
}
```

### 2.4 Hash Table Allocation

```rust
/// A typed reference to a hash table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HashTable(usize);

const HASH_TABLE_ID_BASE: usize = 0x1000_0000;

/// Allocate a hash table ID
pub fn alloc_hash_table<'a>() -> Emit<'a, HashTable> {
    Emit::new(|_, state| {
        let id = HASH_TABLE_ID_BASE + state.next_hash_table;
        state.next_hash_table += 1;
        Ok(HashTable(id))
    })
}
```

---

## Part 3: Instruction Emission

### 3.1 Instruction Specification

Instructions are specified declaratively, with typed references to resources:

```rust
/// Declarative instruction specification
/// Uses typed Reg, Cursor, Label instead of raw usize
pub enum InsnSpec {
    // Control flow
    Init { target: Label },
    Goto { target: Label },
    Halt,

    // Cursor operations
    OpenRead { cursor: Cursor, root_page: usize },
    OpenWrite { cursor: Cursor, root_page: usize },
    OpenPseudo { cursor: Cursor, content_reg: Reg, columns: usize },
    OpenEphemeral { cursor: Cursor, is_table: bool },
    Close { cursor: Cursor },

    // Loop control
    Rewind { cursor: Cursor, if_empty: Label },
    Last { cursor: Cursor, if_empty: Label },
    Next { cursor: Cursor, if_next: Label },
    Prev { cursor: Cursor, if_prev: Label },

    // Data access
    Column { cursor: Cursor, column: usize, dest: Reg },
    RowId { cursor: Cursor, dest: Reg },

    // Data manipulation
    Copy { src: Reg, dest: Reg, count: usize },
    SCopy { src: Reg, dest: Reg },
    Move { src: Reg, dest: Reg, count: usize },

    // Values
    Null { dest: Reg, count: usize },
    Integer { value: i64, dest: Reg },
    Real { value: f64, dest: Reg },
    String8 { value: String, dest: Reg },
    Blob { value: Vec<u8>, dest: Reg },

    // Comparisons
    Eq { lhs: Reg, rhs: Reg, target: Label, null_eq: bool },
    Ne { lhs: Reg, rhs: Reg, target: Label, null_eq: bool },
    Lt { lhs: Reg, rhs: Reg, target: Label, null_eq: bool },
    Le { lhs: Reg, rhs: Reg, target: Label, null_eq: bool },
    Gt { lhs: Reg, rhs: Reg, target: Label, null_eq: bool },
    Ge { lhs: Reg, rhs: Reg, target: Label, null_eq: bool },

    // Branching
    If { reg: Reg, target: Label, null_eq: bool },
    IfNot { reg: Reg, target: Label, null_eq: bool },
    IsNull { reg: Reg, target: Label },
    NotNull { reg: Reg, target: Label },
    Once { target: Label },

    // Results
    ResultRow { start: Reg, count: usize },

    // Aggregation
    AggStep { func: AggFunc, args: RegRange, dest: Reg },
    AggFinal { dest: Reg, func: AggFunc },

    // Sorting
    SorterOpen { cursor: Cursor, columns: usize },
    SorterInsert { cursor: Cursor, record: Reg },
    SorterSort { cursor: Cursor, if_empty: Label },
    SorterData { cursor: Cursor, dest: Reg },
    SorterNext { cursor: Cursor, if_next: Label },

    // Records
    MakeRecord { start: Reg, count: usize, dest: Reg },

    // Insert/Update/Delete
    Insert { cursor: Cursor, key: Reg, record: Reg, flags: InsertFlags },
    Delete { cursor: Cursor },

    // Subqueries
    BeginSubrtn { dest: Reg },
    Return { reg: Reg },
    Yield { reg: Reg, target: Label },
    EndCoroutine { reg: Reg },

    // Misc
    Noop,
    Trace { message: String },
}
```

### 3.2 Emission Primitive

```rust
/// Emit a single instruction
pub fn emit<'a>(insn: InsnSpec) -> Emit<'a, ()> {
    Emit::new(move |_, state| {
        state.instructions.push(insn);
        Ok(())
    })
}

/// Emit multiple instructions in sequence
pub fn emit_all<'a>(insns: Vec<InsnSpec>) -> Emit<'a, ()> {
    Emit::new(move |_, state| {
        state.instructions.extend(insns);
        Ok(())
    })
}

/// Get current instruction position (for forward reference tracking)
pub fn current_pos<'a>() -> Emit<'a, InsnPos> {
    Emit::new(|_, state| Ok(InsnPos(state.instructions.len())))
}
```

### 3.3 Instruction Builder Combinators

```rust
/// Open a cursor for reading and return the cursor reference
pub fn open_read<'a>(table: &Table) -> Emit<'a, Cursor> {
    alloc_table_cursor(table).flat_map(move |cursor| {
        emit(InsnSpec::OpenRead {
            cursor,
            root_page: table.root_page,
        }).map(move |_| cursor)
    })
}

/// Open a cursor for writing
pub fn open_write<'a>(table: &Table) -> Emit<'a, Cursor> {
    alloc_table_cursor(table).flat_map(move |cursor| {
        emit(InsnSpec::OpenWrite {
            cursor,
            root_page: table.root_page,
        }).map(move |_| cursor)
    })
}

/// Read a column value into a new register
pub fn read_column<'a>(cursor: Cursor, column: usize) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |reg| {
        emit(InsnSpec::Column { cursor, column, dest: reg }).map(move |_| reg)
    })
}

/// Read rowid into a new register
pub fn read_rowid<'a>(cursor: Cursor) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |reg| {
        emit(InsnSpec::RowId { cursor, dest: reg }).map(move |_| reg)
    })
}

/// Load an integer constant into a new register
pub fn load_int<'a>(value: i64) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |reg| {
        emit(InsnSpec::Integer { value, dest: reg }).map(move |_| reg)
    })
}

/// Load a string constant into a new register
pub fn load_string<'a>(value: String) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |reg| {
        emit(InsnSpec::String8 { value, dest: reg }).map(move |_| reg)
    })
}

/// Emit a result row from a register range
pub fn result_row<'a>(regs: RegRange) -> Emit<'a, ()> {
    emit(InsnSpec::ResultRow {
        start: Reg(regs.start()),
        count: regs.count()
    })
}
```

---

## Part 4: Control Flow Combinators

### 4.1 Sequential Composition

```rust
/// Run two computations in sequence, keeping the second result
pub fn then<'a, T: 'a, U: 'a>(first: Emit<'a, T>, second: Emit<'a, U>) -> Emit<'a, U> {
    first.flat_map(|_| second)
}

/// Run multiple computations in sequence, keeping all results
pub fn seq<'a, T: 'a>(computations: Vec<Emit<'a, T>>) -> Emit<'a, Vec<T>> {
    sequence(computations)
}

/// Chain builder for readable sequential composition
pub struct EmitChain<'a, T> {
    current: Emit<'a, T>,
}

impl<'a, T: 'a> EmitChain<'a, T> {
    pub fn start(emit: Emit<'a, T>) -> Self {
        EmitChain { current: emit }
    }

    pub fn then<U: 'a>(self, next: Emit<'a, U>) -> EmitChain<'a, U> {
        EmitChain {
            current: self.current.flat_map(|_| next),
        }
    }

    pub fn then_with<U: 'a, F>(self, f: F) -> EmitChain<'a, U>
    where
        F: FnOnce(T) -> Emit<'a, U> + 'a,
    {
        EmitChain {
            current: self.current.flat_map(f),
        }
    }

    pub fn finish(self) -> Emit<'a, T> {
        self.current
    }
}
```

### 4.2 Branching and Conditionals

```rust
/// Conditional emission: emit only if condition is true
pub fn when_<'a, T: 'a + Default>(
    condition: bool,
    computation: Emit<'a, T>,
) -> Emit<'a, T> {
    if condition {
        computation
    } else {
        Emit::pure(T::default())
    }
}

/// Emit if condition, else emit alternative
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

/// Pattern matching helper for Option
pub fn match_option<'a, T, U: 'a>(
    opt: Option<T>,
    some_branch: impl FnOnce(T) -> Emit<'a, U> + 'a,
    none_branch: Emit<'a, U>,
) -> Emit<'a, U> {
    match opt {
        Some(t) => some_branch(t),
        None => none_branch,
    }
}
```

### 4.3 Loop Structures

```rust
/// A declarative loop specification
pub struct Loop<'a> {
    labels: LoopLabels,
    cursor: Cursor,
    direction: LoopDirection,
    body: Box<dyn FnOnce(LoopContext) -> Emit<'a, ()> + 'a>,
}

pub enum LoopDirection {
    Forward,  // Rewind + Next
    Backward, // Last + Prev
}

pub struct LoopContext {
    pub cursor: Cursor,
    pub labels: LoopLabels,
}

/// Build a forward loop over a cursor
pub fn for_each<'a, F>(cursor: Cursor, body: F) -> Emit<'a, ()>
where
    F: FnOnce(LoopContext) -> Emit<'a, ()> + 'a,
{
    alloc_loop_labels().flat_map(move |labels| {
        let ctx = LoopContext { cursor, labels };

        // Rewind cursor, jump to end if empty
        emit(InsnSpec::Rewind {
            cursor,
            if_empty: labels.end
        })
        // Bind loop start label
        .flat_map(move |_| bind_label(labels.start))
        // Execute body
        .flat_map(move |_| body(ctx))
        // Bind next label
        .flat_map(move |_| bind_label(labels.next))
        // Next iteration or exit
        .flat_map(move |_| emit(InsnSpec::Next {
            cursor,
            if_next: labels.start
        }))
        // Bind end label
        .flat_map(move |_| bind_label(labels.end))
    })
}

/// Build a backward loop over a cursor
pub fn for_each_rev<'a, F>(cursor: Cursor, body: F) -> Emit<'a, ()>
where
    F: FnOnce(LoopContext) -> Emit<'a, ()> + 'a,
{
    alloc_loop_labels().flat_map(move |labels| {
        let ctx = LoopContext { cursor, labels };

        emit(InsnSpec::Last { cursor, if_empty: labels.end })
            .flat_map(move |_| bind_label(labels.start))
            .flat_map(move |_| body(ctx))
            .flat_map(move |_| bind_label(labels.next))
            .flat_map(move |_| emit(InsnSpec::Prev { cursor, if_next: labels.start }))
            .flat_map(move |_| bind_label(labels.end))
    })
}

/// Nested loops for joins
pub fn nested_loop<'a, F>(
    outer_cursor: Cursor,
    inner_cursor: Cursor,
    body: F,
) -> Emit<'a, ()>
where
    F: FnOnce(LoopContext, LoopContext) -> Emit<'a, ()> + 'a,
{
    for_each(outer_cursor, move |outer_ctx| {
        for_each(inner_cursor, move |inner_ctx| {
            body(outer_ctx, inner_ctx)
        })
    })
}
```

### 4.4 Scoped Operations

```rust
/// Execute a computation in a nested scope (for subqueries)
pub fn scoped<'a, T: 'a>(computation: Emit<'a, T>) -> Emit<'a, T> {
    Emit::new(move |env, state| {
        state.nesting_depth += 1;
        let result = computation.run(env, state);
        state.nesting_depth -= 1;
        result
    })
}

/// Execute as a subroutine (BeginSubrtn...Return)
pub fn subroutine<'a, F>(body: F) -> Emit<'a, Reg>
where
    F: FnOnce() -> Emit<'a, ()> + 'a,
{
    alloc_reg().flat_map(move |return_reg| {
        emit(InsnSpec::BeginSubrtn { dest: return_reg })
            .flat_map(move |_| scoped(body()))
            .flat_map(move |_| emit(InsnSpec::Return { reg: return_reg }))
            .map(move |_| return_reg)
    })
}

/// Execute as a coroutine (Yield-based iteration)
pub fn coroutine<'a, F>(body: F) -> Emit<'a, Reg>
where
    F: FnOnce(Reg) -> Emit<'a, ()> + 'a,
{
    alloc_reg().flat_map(move |yield_reg| {
        alloc_label().flat_map(move |start_label| {
            emit(InsnSpec::InitCoroutine {
                reg: yield_reg,
                target: start_label
            })
            .flat_map(move |_| bind_label(start_label))
            .flat_map(move |_| scoped(body(yield_reg)))
            .flat_map(move |_| emit(InsnSpec::EndCoroutine { reg: yield_reg }))
            .map(move |_| yield_reg)
        })
    })
}
```

---

## Part 5: Metadata Accumulation

### 5.1 Metadata Types

```rust
/// Accumulated metadata during emission
#[derive(Default)]
pub struct EmitMetadata {
    pub group_by: Option<GroupByMeta>,
    pub order_by: Option<OrderByMeta>,
    pub window: Option<WindowMeta>,
    pub limit: Option<LimitMeta>,
    pub left_joins: Vec<LeftJoinMeta>,
    pub result_regs: Option<RegRange>,
    pub agg_start: Option<Reg>,
}

pub struct GroupByMeta {
    pub sorter: Cursor,
    pub key_regs: RegRange,
    pub agg_regs: RegRange,
    pub output_regs: RegRange,
}

pub struct OrderByMeta {
    pub sorter: Cursor,
    pub key_count: usize,
}

pub struct LimitMeta {
    pub limit_reg: Option<Reg>,
    pub offset_reg: Option<Reg>,
    pub counter_reg: Reg,
}

pub struct LeftJoinMeta {
    pub match_flag: Reg,
    pub set_label: Label,
    pub check_label: Label,
}
```

### 5.2 Metadata Combinators

```rust
/// Set GROUP BY metadata
pub fn set_group_by<'a>(meta: GroupByMeta) -> Emit<'a, ()> {
    Emit::new(move |_, state| {
        state.metadata.group_by = Some(meta);
        Ok(())
    })
}

/// Get GROUP BY metadata (fails if not set)
pub fn get_group_by<'a>() -> Emit<'a, GroupByMeta> {
    Emit::new(|_, state| {
        state.metadata.group_by.clone()
            .ok_or_else(|| LimboError::InternalError(
                "GROUP BY metadata not initialized".into()
            ))
    })
}

/// Set ORDER BY metadata
pub fn set_order_by<'a>(meta: OrderByMeta) -> Emit<'a, ()> {
    Emit::new(move |_, state| {
        state.metadata.order_by = Some(meta);
        Ok(())
    })
}

/// Set result column registers
pub fn set_result_regs<'a>(regs: RegRange) -> Emit<'a, ()> {
    Emit::new(move |_, state| {
        state.metadata.result_regs = Some(regs);
        Ok(())
    })
}

/// Get result column registers
pub fn get_result_regs<'a>() -> Emit<'a, RegRange> {
    Emit::new(|_, state| {
        state.metadata.result_regs
            .ok_or_else(|| LimboError::InternalError(
                "Result registers not initialized".into()
            ))
    })
}

/// Add a left join metadata entry
pub fn add_left_join<'a>(meta: LeftJoinMeta) -> Emit<'a, usize> {
    Emit::new(move |_, state| {
        let index = state.metadata.left_joins.len();
        state.metadata.left_joins.push(meta);
        Ok(index)
    })
}
```

---

## Part 6: Expression Translation

### 6.1 Expression Emitter

```rust
/// Translate an expression to bytecode, returning the result register
pub fn translate_expr<'a>(
    expr: &'a ast::Expr,
    tables: Option<&'a TableReferences>,
) -> Emit<'a, Reg> {
    match expr {
        ast::Expr::Literal(lit) => translate_literal(lit),
        ast::Expr::Column { table, column, .. } => translate_column(tables, table, column),
        ast::Expr::Binary { lhs, op, rhs } => translate_binary(lhs, op, rhs, tables),
        ast::Expr::Unary { op, expr } => translate_unary(op, expr, tables),
        ast::Expr::Function { name, args, .. } => translate_function(name, args, tables),
        ast::Expr::Subquery { query, .. } => translate_subquery(query),
        ast::Expr::Case { operand, when_clauses, else_clause } => {
            translate_case(operand, when_clauses, else_clause, tables)
        }
        ast::Expr::Cast { expr, type_name } => translate_cast(expr, type_name, tables),
        ast::Expr::InList { lhs, list, not } => translate_in_list(lhs, list, *not, tables),
        // ... other expression types
    }
}

fn translate_literal<'a>(lit: &ast::Literal) -> Emit<'a, Reg> {
    match lit {
        ast::Literal::Numeric(n) => {
            if let Ok(i) = n.parse::<i64>() {
                load_int(i)
            } else {
                let f = n.parse::<f64>().unwrap();
                alloc_reg().flat_map(move |reg| {
                    emit(InsnSpec::Real { value: f, dest: reg }).map(move |_| reg)
                })
            }
        }
        ast::Literal::String(s) => load_string(s.clone()),
        ast::Literal::Null => alloc_reg_null(),
        ast::Literal::Blob(b) => {
            alloc_reg().flat_map(move |reg| {
                emit(InsnSpec::Blob { value: b.clone(), dest: reg }).map(move |_| reg)
            })
        }
        ast::Literal::CurrentTime => translate_function("current_time", &[], None),
        ast::Literal::CurrentDate => translate_function("current_date", &[], None),
        ast::Literal::CurrentTimestamp => translate_function("current_timestamp", &[], None),
    }
}

fn translate_binary<'a>(
    lhs: &'a ast::Expr,
    op: &ast::BinaryOp,
    rhs: &'a ast::Expr,
    tables: Option<&'a TableReferences>,
) -> Emit<'a, Reg> {
    translate_expr(lhs, tables).flat_map(move |lhs_reg| {
        translate_expr(rhs, tables).flat_map(move |rhs_reg| {
            alloc_reg().flat_map(move |dest| {
                let func = match op {
                    ast::BinaryOp::Add => ScalarFunc::Add,
                    ast::BinaryOp::Subtract => ScalarFunc::Subtract,
                    ast::BinaryOp::Multiply => ScalarFunc::Multiply,
                    ast::BinaryOp::Divide => ScalarFunc::Divide,
                    ast::BinaryOp::Modulo => ScalarFunc::Modulo,
                    ast::BinaryOp::Concat => ScalarFunc::Concat,
                    // ... comparison operators handled differently
                    _ => return translate_comparison(lhs_reg, op, rhs_reg),
                };
                emit(InsnSpec::Function {
                    func,
                    args: vec![lhs_reg, rhs_reg],
                    dest,
                }).map(move |_| dest)
            })
        })
    })
}
```

### 6.2 Conditional Expression Translation

```rust
fn translate_case<'a>(
    operand: &'a Option<Box<ast::Expr>>,
    when_clauses: &'a [(ast::Expr, ast::Expr)],
    else_clause: &'a Option<Box<ast::Expr>>,
    tables: Option<&'a TableReferences>,
) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |result_reg| {
        alloc_label().flat_map(move |end_label| {
            // Translate operand if present
            let operand_emit = match operand {
                Some(op) => translate_expr(op, tables).map(Some),
                None => Emit::pure(None),
            };

            operand_emit.flat_map(move |operand_reg| {
                // Translate each WHEN clause
                let clauses = when_clauses.iter().fold(
                    Emit::pure(()),
                    move |acc, (when_expr, then_expr)| {
                        acc.flat_map(move |_| {
                            alloc_label().flat_map(move |next_when| {
                                // Translate condition
                                let cond_emit = match operand_reg {
                                    Some(op_reg) => {
                                        translate_expr(when_expr, tables).flat_map(move |when_reg| {
                                            emit(InsnSpec::Ne {
                                                lhs: op_reg,
                                                rhs: when_reg,
                                                target: next_when,
                                                null_eq: false,
                                            })
                                        })
                                    }
                                    None => {
                                        translate_expr(when_expr, tables).flat_map(move |cond_reg| {
                                            emit(InsnSpec::IfNot {
                                                reg: cond_reg,
                                                target: next_when,
                                                null_eq: false,
                                            })
                                        })
                                    }
                                };

                                cond_emit
                                    // Translate THEN expression
                                    .flat_map(move |_| translate_expr(then_expr, tables))
                                    .flat_map(move |then_reg| {
                                        emit(InsnSpec::SCopy { src: then_reg, dest: result_reg })
                                    })
                                    .flat_map(move |_| emit(InsnSpec::Goto { target: end_label }))
                                    .flat_map(move |_| bind_label(next_when))
                            })
                        })
                    },
                );

                clauses.flat_map(move |_| {
                    // Translate ELSE clause
                    match else_clause {
                        Some(else_expr) => {
                            translate_expr(else_expr, tables).flat_map(move |else_reg| {
                                emit(InsnSpec::SCopy { src: else_reg, dest: result_reg })
                            })
                        }
                        None => emit(InsnSpec::Null { dest: result_reg, count: 1 }),
                    }
                })
                .flat_map(move |_| bind_label(end_label))
                .map(move |_| result_reg)
            })
        })
    })
}
```

---

## Part 7: High-Level Query Emission

### 7.1 SELECT Query

```rust
/// Emit a complete SELECT query
pub fn emit_select<'a>(plan: &'a SelectPlan) -> Emit<'a, RegRange> {
    EmitChain::start(alloc_label())
        .then_with(|main_loop_end| {
            // Initialize result registers
            alloc_regs(plan.result_columns.len()).flat_map(move |result_regs| {
                set_result_regs(result_regs).map(move |_| (main_loop_end, result_regs))
            })
        })
        // Initialize ORDER BY if present
        .then_with(|(main_loop_end, result_regs)| {
            init_order_by_if_needed(plan)
                .map(move |_| (main_loop_end, result_regs))
        })
        // Initialize GROUP BY if present
        .then_with(|(main_loop_end, result_regs)| {
            init_group_by_if_needed(plan)
                .map(move |_| (main_loop_end, result_regs))
        })
        // Initialize LIMIT if present
        .then_with(|(main_loop_end, result_regs)| {
            init_limit_if_needed(plan)
                .map(move |_| (main_loop_end, result_regs))
        })
        // Emit main loop
        .then_with(|(main_loop_end, result_regs)| {
            emit_main_loop(plan).map(move |_| (main_loop_end, result_regs))
        })
        // Bind main loop end label
        .then_with(|(main_loop_end, result_regs)| {
            bind_label(main_loop_end).map(move |_| result_regs)
        })
        // Post-loop processing
        .then_with(|result_regs| {
            emit_post_loop(plan).map(move |_| result_regs)
        })
        .finish()
}

fn emit_main_loop<'a>(plan: &'a SelectPlan) -> Emit<'a, ()> {
    // Open all cursors
    open_cursors(&plan.table_references).flat_map(|cursors| {
        // Build nested loop structure based on join order
        build_join_loop(&plan.join_order, &cursors, |ctx| {
            // Filter rows
            emit_where_filter(plan, &ctx).flat_map(move |_| {
                // Emit result or aggregate
                if plan.has_aggregates() {
                    emit_agg_step(plan, &ctx)
                } else {
                    emit_result_row(plan, &ctx)
                }
            })
        })
    })
}

fn build_join_loop<'a, F>(
    join_order: &'a [JoinOrderEntry],
    cursors: &HashMap<TableId, Cursor>,
    body: F,
) -> Emit<'a, ()>
where
    F: FnOnce(JoinContext) -> Emit<'a, ()> + Clone + 'a,
{
    if join_order.is_empty() {
        return body(JoinContext::empty());
    }

    let first = &join_order[0];
    let cursor = cursors[&first.table_id];
    let rest = &join_order[1..];

    for_each(cursor, move |loop_ctx| {
        if rest.is_empty() {
            body(JoinContext::single(loop_ctx))
        } else {
            build_join_loop(rest, cursors, body.clone())
        }
    })
}
```

### 7.2 INSERT Statement

```rust
pub fn emit_insert<'a>(plan: &'a InsertPlan) -> Emit<'a, ()> {
    // Open table cursor for writing
    open_write(&plan.table).flat_map(|table_cursor| {
        // Open index cursors
        traverse(plan.indexes.clone(), |idx| open_write_index(&idx))
            .flat_map(move |index_cursors| {
                // For each row in VALUES or SELECT
                match &plan.source {
                    InsertSource::Values(rows) => {
                        traverse(rows.clone(), move |row| {
                            emit_insert_row(
                                table_cursor,
                                &index_cursors,
                                &plan.columns,
                                &row,
                            )
                        }).map(|_| ())
                    }
                    InsertSource::Select(select_plan) => {
                        emit_select(select_plan).flat_map(move |result_regs| {
                            emit_insert_from_select(
                                table_cursor,
                                &index_cursors,
                                &plan.columns,
                                result_regs,
                            )
                        })
                    }
                }
            })
    })
}

fn emit_insert_row<'a>(
    table_cursor: Cursor,
    index_cursors: &[Cursor],
    columns: &[Column],
    values: &[ast::Expr],
) -> Emit<'a, ()> {
    // Allocate registers for all values
    alloc_regs(values.len()).flat_map(move |value_regs| {
        // Translate each value expression
        traverse(
            values.iter().enumerate().collect(),
            move |(i, expr)| {
                translate_expr(expr, None).flat_map(move |src| {
                    emit(InsnSpec::SCopy {
                        src,
                        dest: value_regs.get(i)
                    })
                })
            },
        )
        .flat_map(move |_| {
            // Generate rowid
            alloc_reg().flat_map(move |rowid_reg| {
                emit(InsnSpec::NewRowId {
                    cursor: table_cursor,
                    dest: rowid_reg
                })
                .flat_map(move |_| {
                    // Make record
                    alloc_reg().flat_map(move |record_reg| {
                        emit(InsnSpec::MakeRecord {
                            start: Reg(value_regs.start()),
                            count: value_regs.count(),
                            dest: record_reg,
                        })
                        .flat_map(move |_| {
                            // Insert into main table
                            emit(InsnSpec::Insert {
                                cursor: table_cursor,
                                key: rowid_reg,
                                record: record_reg,
                                flags: InsertFlags::default(),
                            })
                        })
                        // Insert into each index
                        .flat_map(move |_| {
                            traverse(
                                index_cursors.to_vec(),
                                move |idx_cursor| {
                                    emit_index_insert(
                                        idx_cursor,
                                        rowid_reg,
                                        value_regs,
                                    )
                                },
                            ).map(|_| ())
                        })
                    })
                })
            })
        })
    })
}
```

---

## Part 8: Program Finalization

### 8.1 Lowering to ProgramBuilder

```rust
/// Lower the declarative instruction buffer to concrete bytecode
pub fn lower_to_program(state: EmitState) -> Result<Program> {
    let mut builder = ProgramBuilder::new();

    // First pass: emit all instructions with placeholder jumps
    for insn in &state.instructions {
        let concrete = lower_instruction(insn, &state.labels)?;
        builder.emit_insn(concrete);
    }

    // Second pass: resolve all label references
    for (label, label_state) in state.labels.iter() {
        match label_state {
            LabelState::Resolved(pos) => {
                // Already resolved during emission
            }
            LabelState::Unresolved { forward_refs } => {
                return Err(LimboError::InternalError(format!(
                    "Unresolved label {:?}",
                    label
                )));
            }
        }
    }

    builder.build()
}

fn lower_instruction(insn: &InsnSpec, labels: &LabelTable) -> Result<Insn> {
    match insn {
        InsnSpec::Rewind { cursor, if_empty } => {
            Ok(Insn::Rewind {
                cursor_id: cursor.id(),
                pc_if_empty: labels.resolve(*if_empty)?,
            })
        }
        InsnSpec::Next { cursor, if_next } => {
            Ok(Insn::Next {
                cursor_id: cursor.id(),
                pc_if_next: labels.resolve(*if_next)?,
            })
        }
        InsnSpec::Column { cursor, column, dest } => {
            Ok(Insn::Column {
                cursor_id: cursor.id(),
                column: *column,
                dest: dest.index(),
            })
        }
        // ... other instruction translations
    }
}
```

### 8.2 Complete Emission Pipeline

```rust
/// Main entry point: translate a statement to a program
pub fn translate_statement<'a>(
    stmt: &'a ast::Stmt,
    env: &EmitEnv<'a>,
) -> Result<Program> {
    let mut state = EmitState::new();

    // Build the emission computation
    let computation = match stmt {
        ast::Stmt::Select(select) => {
            let plan = build_select_plan(select, env)?;
            emit_select(&plan).map(|_| ())
        }
        ast::Stmt::Insert(insert) => {
            let plan = build_insert_plan(insert, env)?;
            emit_insert(&plan)
        }
        ast::Stmt::Update(update) => {
            let plan = build_update_plan(update, env)?;
            emit_update(&plan)
        }
        ast::Stmt::Delete(delete) => {
            let plan = build_delete_plan(delete, env)?;
            emit_delete(&plan)
        }
        // ... other statement types
    };

    // Add program wrapper (Init + Halt)
    let program_computation = emit_program_wrapper(computation);

    // Execute the computation
    program_computation.run(env, &mut state)?;

    // Lower to concrete program
    lower_to_program(state)
}

fn emit_program_wrapper<'a>(body: Emit<'a, ()>) -> Emit<'a, ()> {
    alloc_label().flat_map(|start_label| {
        emit(InsnSpec::Init { target: start_label })
            .flat_map(move |_| bind_label(start_label))
            .flat_map(move |_| body)
            .flat_map(|_| emit(InsnSpec::Halt))
    })
}
```

---

## Part 9: Do-Notation Macro

To improve ergonomics, we provide a do-notation macro:

```rust
/// Do-notation macro for Emit monad
///
/// Usage:
/// ```rust
/// emit_do! {
///     cursor <- open_read(&table);
///     rowid_reg <- read_rowid(cursor);
///     for_each(cursor, |ctx| emit_do! {
///         col_reg <- read_column(ctx.cursor, 0);
///         _ <- emit(InsnSpec::ResultRow { start: col_reg, count: 1 });
///         pure(())
///     })
/// }
/// ```
#[macro_export]
macro_rules! emit_do {
    // Base case: final expression
    (pure($e:expr)) => {
        Emit::pure($e)
    };

    // Final expression without pure
    ($e:expr) => {
        $e
    };

    // Let binding (pure value, not monadic)
    (let $p:pat = $e:expr; $($rest:tt)*) => {
        {
            let $p = $e;
            emit_do!($($rest)*)
        }
    };

    // Monadic binding with pattern
    ($p:pat <- $e:expr; $($rest:tt)*) => {
        $e.flat_map(move |$p| emit_do!($($rest)*))
    };

    // Monadic action (discard result)
    (_ <- $e:expr; $($rest:tt)*) => {
        $e.flat_map(move |_| emit_do!($($rest)*))
    };

    // Conditional
    (if $cond:expr { $($then:tt)* } else { $($else:tt)* }; $($rest:tt)*) => {
        if_then_else(
            $cond,
            emit_do!($($then)*),
            emit_do!($($else)*),
        ).flat_map(move |_| emit_do!($($rest)*))
    };

    // Conditional with binding
    ($p:pat <- if $cond:expr { $($then:tt)* } else { $($else:tt)* }; $($rest:tt)*) => {
        if_then_else(
            $cond,
            emit_do!($($then)*),
            emit_do!($($else)*),
        ).flat_map(move |$p| emit_do!($($rest)*))
    };
}
```

### Example Usage

```rust
fn emit_simple_select(table: &Table, columns: &[usize]) -> Emit<'_, RegRange> {
    emit_do! {
        // Open cursor
        cursor <- open_read(table);

        // Allocate result registers
        result_regs <- alloc_regs(columns.len());

        // Main loop
        _ <- for_each(cursor, |ctx| emit_do! {
            // Read each column
            _ <- traverse(columns.iter().enumerate().collect(), |(i, &col)| emit_do! {
                val <- read_column(ctx.cursor, col);
                _ <- emit(InsnSpec::SCopy {
                    src: val,
                    dest: result_regs.get(i)
                });
                pure(())
            });

            // Emit result row
            _ <- result_row(result_regs);
            pure(())
        });

        pure(result_regs)
    }
}
```

---

## Part 10: Migration Strategy

### 10.1 Incremental Adoption

The monadic emitter can coexist with the imperative code:

```rust
// Bridge: run monadic computation in imperative context
pub fn run_emit_in_context<'a, T>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    emit: Emit<'a, T>,
) -> Result<T> {
    // Create EmitEnv from existing context
    let env = EmitEnv {
        schema: t_ctx.resolver.schema,
        symbol_table: t_ctx.resolver.symbol_table,
        connection: /* ... */,
    };

    // Create EmitState synchronized with ProgramBuilder
    let mut state = EmitState::from_program_builder(program);

    // Run the computation
    let result = emit.run(&env, &mut state)?;

    // Sync state back to ProgramBuilder
    state.sync_to_program_builder(program);

    Ok(result)
}
```

### 10.2 Migration Path

1. **Phase 1**: Implement core monad and primitives
2. **Phase 2**: Add expression translation (`translate_expr`)
3. **Phase 3**: Add loop combinators
4. **Phase 4**: Migrate simple queries (no GROUP BY/ORDER BY)
5. **Phase 5**: Add aggregation and sorting
6. **Phase 6**: Migrate complex queries
7. **Phase 7**: Remove imperative code

---

## Part 11: Benefits Summary

| Aspect | Imperative (Current) | Monadic (Proposed) |
|--------|---------------------|-------------------|
| **State management** | Mutable refs passed everywhere | Encapsulated in monad |
| **Error handling** | Manual `?` threading | Automatic short-circuit |
| **Composability** | Function calls with side effects | Pure function composition |
| **Testing** | Requires full ProgramBuilder mock | Test individual computations |
| **Readability** | Nested callbacks and mutations | Linear do-notation |
| **Label management** | Manual allocate/resolve/backpatch | Automatic via monad |
| **Register tracking** | Manual counter increments | Type-safe Reg references |
| **Refactoring** | Risky (hidden dependencies) | Safe (explicit dependencies) |

---

## Appendix A: Complete Type Definitions

```rust
// Core types
pub struct Emit<'a, T>;
pub struct EmitEnv<'a>;
pub struct EmitState;

// Resource references
pub struct Reg(usize);
pub struct RegRange { start: usize, count: usize };
pub struct Cursor(usize);
pub struct Label(usize);
pub struct HashTable(usize);

// Control structures
pub struct LoopLabels { start: Label, next: Label, end: Label };
pub struct LoopContext { cursor: Cursor, labels: LoopLabels };

// Metadata
pub struct GroupByMeta;
pub struct OrderByMeta;
pub struct LimitMeta;
pub struct LeftJoinMeta;
pub struct EmitMetadata;

// Instructions
pub enum InsnSpec;
pub enum CursorKind;
```

---

## Appendix B: Monad Laws Verification

The implementation must satisfy the monad laws:

```rust
#[cfg(test)]
mod laws {
    // Left identity: pure(a).flat_map(f) ≡ f(a)
    #[test]
    fn left_identity() {
        let a = 42;
        let f = |x| Emit::pure(x * 2);

        let lhs = Emit::pure(a).flat_map(f);
        let rhs = f(a);

        assert_emit_eq(lhs, rhs);
    }

    // Right identity: m.flat_map(pure) ≡ m
    #[test]
    fn right_identity() {
        let m = alloc_reg();

        let lhs = m.clone().flat_map(Emit::pure);
        let rhs = m;

        assert_emit_eq(lhs, rhs);
    }

    // Associativity: m.flat_map(f).flat_map(g) ≡ m.flat_map(|x| f(x).flat_map(g))
    #[test]
    fn associativity() {
        let m = alloc_reg();
        let f = |r: Reg| load_int(r.index() as i64);
        let g = |r: Reg| emit(InsnSpec::SCopy { src: r, dest: Reg(0) });

        let lhs = m.clone().flat_map(f).flat_map(g);
        let rhs = m.flat_map(|x| f(x).flat_map(g));

        assert_emit_eq(lhs, rhs);
    }
}
```
