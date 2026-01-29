//! Resource allocation primitives for the monadic emitter.
//!
//! This module provides monadic operations for allocating resources:
//! - Registers (single and ranges)
//! - Cursors (with metadata)
//! - Labels (for jump targets)
//! - Hash tables

// Note: This module provides a public API for resource allocation.
// Many functions may not be used internally yet but are provided for consumers.
#![allow(dead_code)]

use crate::schema::{BTreeTable, Index};

use super::insn::InsnSpec;
use super::types::{
    Cursor, CursorKind, Emit, HashTableId, InsnPos, Label, LoopLabels, Reg, RegRange,
    HASH_TABLE_ID_BASE,
};

// =============================================================================
// Register Allocation
// =============================================================================

/// Allocate a single register.
pub fn alloc_reg<'a>() -> Emit<'a, Reg> {
    Emit::new(|_, state| {
        let reg = Reg(state.next_register);
        state.next_register += 1;
        Ok(reg)
    })
}

/// Allocate a contiguous range of registers.
pub fn alloc_regs<'a>(count: usize) -> Emit<'a, RegRange> {
    Emit::new(move |_, state| {
        let start = state.next_register;
        state.next_register += count;
        Ok(RegRange { start, count })
    })
}

/// Allocate a register and initialize it to NULL.
pub fn alloc_reg_null<'a>() -> Emit<'a, Reg> {
    alloc_reg().flat_map(|reg| {
        emit(InsnSpec::Null {
            dest: reg,
            count: 1,
        })
        .map(move |_| reg)
    })
}

/// Allocate a range of registers and initialize them to NULL.
pub fn alloc_regs_null<'a>(count: usize) -> Emit<'a, RegRange> {
    alloc_regs(count).flat_map(move |range| {
        emit(InsnSpec::Null {
            dest: range.first(),
            count,
        })
        .map(move |_| range)
    })
}

/// Allocate a register and initialize it with an integer value.
pub fn alloc_reg_int<'a>(value: i64) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |reg| emit(InsnSpec::Integer { value, dest: reg }).map(move |_| reg))
}

/// Allocate a register and initialize it with a string value.
pub fn alloc_reg_string<'a>(value: String) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |reg| emit(InsnSpec::String8 { value, dest: reg }).map(move |_| reg))
}

/// Allocate a register and initialize it with a real value.
pub fn alloc_reg_real<'a>(value: f64) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |reg| emit(InsnSpec::Real { value, dest: reg }).map(move |_| reg))
}

// =============================================================================
// Cursor Allocation
// =============================================================================

/// Allocate a cursor ID without metadata.
pub fn alloc_cursor<'a>() -> Emit<'a, Cursor> {
    Emit::new(|_, state| {
        let cursor = Cursor(state.next_cursor);
        state.next_cursor += 1;
        Ok(cursor)
    })
}

/// Allocate a cursor with metadata.
pub fn alloc_cursor_with_kind<'a>(kind: CursorKind) -> Emit<'a, Cursor> {
    Emit::new(move |_, state| {
        let cursor = Cursor(state.next_cursor);
        state.next_cursor += 1;
        state.cursors.register(cursor, kind);
        Ok(cursor)
    })
}

/// Allocate a cursor for a BTree table.
pub fn alloc_table_cursor<'a>(table: &BTreeTable) -> Emit<'a, Cursor> {
    let root_page = table.root_page as usize;
    let table_name = table.name.clone();
    alloc_cursor_with_kind(CursorKind::BTreeTable {
        root_page,
        table_name,
    })
}

/// Allocate a cursor for an index.
pub fn alloc_index_cursor<'a>(index: &Index) -> Emit<'a, Cursor> {
    let root_page = index.root_page as usize;
    let index_name = index.name.clone();
    alloc_cursor_with_kind(CursorKind::BTreeIndex {
        root_page,
        index_name,
    })
}

/// Allocate a sorter cursor.
pub fn alloc_sorter_cursor<'a>(num_columns: usize) -> Emit<'a, Cursor> {
    alloc_cursor_with_kind(CursorKind::Sorter { num_columns })
}

/// Allocate an ephemeral table cursor.
pub fn alloc_ephemeral_cursor<'a>(is_table: bool) -> Emit<'a, Cursor> {
    alloc_cursor_with_kind(CursorKind::Ephemeral { is_table })
}

/// Allocate a pseudo cursor.
pub fn alloc_pseudo_cursor<'a>(content_reg: Reg, num_columns: usize) -> Emit<'a, Cursor> {
    alloc_cursor_with_kind(CursorKind::Pseudo {
        content_reg,
        num_columns,
    })
}

// =============================================================================
// Label Allocation
// =============================================================================

/// Allocate a new label (unresolved).
pub fn alloc_label<'a>() -> Emit<'a, Label> {
    Emit::new(|_, state| {
        let label = state.labels.allocate();
        Ok(label)
    })
}

/// Allocate a label and immediately bind it to the current position.
///
/// This is useful for marking a position that will be jumped to later.
pub fn here<'a>() -> Emit<'a, Label> {
    alloc_label().flat_map(|label| bind_label(label).map(move |_| label))
}

/// Bind a label to the current instruction position.
pub fn bind_label<'a>(label: Label) -> Emit<'a, ()> {
    Emit::new(move |_, state| {
        let pos = InsnPos(state.instructions.len());
        state.labels.resolve(label, pos)
    })
}

/// Allocate a complete set of loop labels.
pub fn alloc_loop_labels<'a>() -> Emit<'a, LoopLabels> {
    alloc_label().flat_map(|start| {
        alloc_label()
            .flat_map(move |next| alloc_label().map(move |end| LoopLabels { start, next, end }))
    })
}

// =============================================================================
// Hash Table Allocation
// =============================================================================

/// Allocate a hash table ID.
pub fn alloc_hash_table<'a>() -> Emit<'a, HashTableId> {
    Emit::new(|_, state| {
        let id = HASH_TABLE_ID_BASE + state.next_hash_table;
        state.next_hash_table += 1;
        Ok(HashTableId(id))
    })
}

// =============================================================================
// Instruction Emission
// =============================================================================

/// Emit a single instruction.
pub fn emit<'a>(insn: InsnSpec) -> Emit<'a, ()> {
    Emit::new(move |_, state| {
        state.instructions.push(insn);
        Ok(())
    })
}

/// Emit multiple instructions in sequence.
pub fn emit_all<'a>(insns: Vec<InsnSpec>) -> Emit<'a, ()> {
    Emit::new(move |_, state| {
        state.instructions.extend(insns);
        Ok(())
    })
}

/// Get the current instruction position.
pub fn current_pos<'a>() -> Emit<'a, InsnPos> {
    Emit::new(|_, state| Ok(InsnPos(state.instructions.len())))
}

// =============================================================================
// Convenience Emission Functions
// =============================================================================

/// Emit a NULL instruction for a single register.
pub fn emit_null<'a>(dest: Reg) -> Emit<'a, ()> {
    emit(InsnSpec::Null { dest, count: 1 })
}

/// Emit a NULL instruction for multiple registers.
pub fn emit_nulls<'a>(dest: Reg, count: usize) -> Emit<'a, ()> {
    emit(InsnSpec::Null { dest, count })
}

/// Emit an integer constant.
pub fn emit_int<'a>(value: i64, dest: Reg) -> Emit<'a, ()> {
    emit(InsnSpec::Integer { value, dest })
}

/// Emit a string constant.
pub fn emit_string<'a>(value: String, dest: Reg) -> Emit<'a, ()> {
    emit(InsnSpec::String8 { value, dest })
}

/// Emit a copy instruction.
pub fn emit_copy<'a>(src: Reg, dest: Reg) -> Emit<'a, ()> {
    emit(InsnSpec::SCopy { src, dest })
}

/// Emit a result row.
pub fn emit_result_row<'a>(start: Reg, count: usize) -> Emit<'a, ()> {
    emit(InsnSpec::ResultRow {
        start_reg: start,
        count,
    })
}

/// Emit result row from a register range.
pub fn emit_result_row_range<'a>(range: RegRange) -> Emit<'a, ()> {
    emit(InsnSpec::ResultRow {
        start_reg: range.first(),
        count: range.count(),
    })
}

/// Emit a goto instruction.
pub fn emit_goto<'a>(target: Label) -> Emit<'a, ()> {
    emit(InsnSpec::Goto { target })
}

/// Emit a halt instruction.
pub fn emit_halt<'a>() -> Emit<'a, ()> {
    emit(InsnSpec::Halt {
        err_code: 0,
        description: String::new(),
    })
}

/// Emit a halt with error.
pub fn emit_halt_error<'a>(err_code: usize, description: String) -> Emit<'a, ()> {
    emit(InsnSpec::Halt {
        err_code,
        description,
    })
}

// =============================================================================
// Cursor Operations
// =============================================================================

/// Emit instruction to read a column into a register.
pub fn emit_column<'a>(cursor: Cursor, column: usize, dest: Reg) -> Emit<'a, ()> {
    emit(InsnSpec::Column {
        cursor,
        column,
        dest,
    })
}

/// Read a column into a newly allocated register.
pub fn read_column<'a>(cursor: Cursor, column: usize) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |dest| {
        emit(InsnSpec::Column {
            cursor,
            column,
            dest,
        })
        .map(move |_| dest)
    })
}

/// Emit instruction to read the rowid.
pub fn emit_rowid<'a>(cursor: Cursor, dest: Reg) -> Emit<'a, ()> {
    emit(InsnSpec::RowId { cursor, dest })
}

/// Read the rowid into a newly allocated register.
pub fn read_rowid<'a>(cursor: Cursor) -> Emit<'a, Reg> {
    alloc_reg().flat_map(move |dest| emit(InsnSpec::RowId { cursor, dest }).map(move |_| dest))
}

// =============================================================================
// Nesting Control
// =============================================================================

/// Execute a computation in a nested scope (for subqueries).
///
/// Increments the nesting depth before running the computation,
/// and decrements it after (even on error).
pub fn scoped<'a, T: 'a>(computation: Emit<'a, T>) -> Emit<'a, T> {
    Emit::new(move |env, state| {
        state.nesting_depth += 1;
        let result = computation.run(env, state);
        state.nesting_depth -= 1;
        result
    })
}

/// Get the current nesting depth.
pub fn nesting_depth<'a>() -> Emit<'a, usize> {
    Emit::new(|_, state| Ok(state.nesting_depth))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::monadic::types::test_helpers::TestEnv;

    #[test]
    fn test_alloc_reg() {
        let env = TestEnv::new();
        let (reg, state) = env.run(alloc_reg()).unwrap();

        assert_eq!(reg.index(), 1); // Registers start at 1
        assert_eq!(state.next_register, 2);
    }

    #[test]
    fn test_alloc_regs() {
        let env = TestEnv::new();
        let (range, state) = env.run(alloc_regs(5)).unwrap();

        assert_eq!(range.start(), 1);
        assert_eq!(range.count(), 5);
        assert_eq!(state.next_register, 6);
    }

    #[test]
    fn test_alloc_label_and_bind() {
        let env = TestEnv::new();

        // Allocate a label, emit two noops, then bind the label
        let computation = alloc_label().flat_map(|label| {
            emit(InsnSpec::Noop).flat_map(move |_| {
                emit(InsnSpec::Noop).flat_map(move |_| bind_label(label).map(move |_| label))
            })
        });

        let (label, state) = env.run(computation).unwrap();

        assert!(state.labels.all_resolved());
        let resolved = state.labels.get_resolved(label).unwrap();
        assert_eq!(resolved.offset(), 2); // After the two Noops
    }

    #[test]
    fn test_emit_instruction() {
        let env = TestEnv::new();

        let computation = emit(InsnSpec::Noop).flat_map(|_| {
            emit(InsnSpec::Integer {
                value: 42,
                dest: Reg(0),
            })
        });

        let (_, state) = env.run(computation).unwrap();
        assert_eq!(state.instructions.len(), 2);
    }

    #[test]
    fn test_alloc_loop_labels() {
        let env = TestEnv::new();
        let (labels, _state) = env.run(alloc_loop_labels()).unwrap();

        // Should have allocated 3 distinct labels
        assert_ne!(labels.start.number(), labels.next.number());
        assert_ne!(labels.next.number(), labels.end.number());
        assert_ne!(labels.start.number(), labels.end.number());
    }

    #[test]
    fn test_scoped_nesting() {
        let env = TestEnv::new();

        // Test that scoped properly increments/decrements nesting depth
        let computation = nesting_depth().flat_map(|d1| {
            scoped(nesting_depth().map(move |d2| (d1, d2)))
                .flat_map(move |(d1, d2)| nesting_depth().map(move |d3| (d1, d2, d3)))
        });

        let ((d1, d2, d3), _state) = env.run(computation).unwrap();
        assert_eq!(d1, 0); // Before scoped
        assert_eq!(d2, 1); // Inside scoped
        assert_eq!(d3, 0); // After scoped
    }

    #[test]
    fn test_alloc_reg_with_init() {
        let env = TestEnv::new();
        let (reg, state) = env.run(alloc_reg_int(42)).unwrap();

        // Should have emitted an Integer instruction
        assert_eq!(state.instructions.len(), 1);
        match &state.instructions[0] {
            InsnSpec::Integer { value, dest } => {
                assert_eq!(*value, 42);
                assert_eq!(*dest, reg);
            }
            _ => panic!("Expected Integer instruction"),
        }
    }
}
