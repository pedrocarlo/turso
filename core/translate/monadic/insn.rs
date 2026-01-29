//! Declarative instruction specification for the monadic emitter.
//!
//! `InsnSpec` is a typed representation of VDBE instructions that uses
//! our type-safe resource references (`Reg`, `Cursor`, `Label`) instead
//! of raw `usize` values.

// Note: InsnSpec variants and fields are provided for the public API.
// They may not all be used internally yet but are provided for consumers.
#![allow(dead_code)]

use std::sync::Arc;

use crate::function::AggFunc;
use crate::schema::BTreeTable;
use crate::translate::collate::CollationSeq;
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags};
use crate::vdbe::PageIdx;

use super::types::{Cursor, Label, Reg};

/// Declarative instruction specification.
///
/// This enum mirrors `vdbe::insn::Insn` but uses typed resource references
/// for type safety and better composability in the monadic emitter.
#[derive(Debug, Clone)]
pub enum InsnSpec {
    // =========================================================================
    // Control Flow
    // =========================================================================
    /// Initialize the program state and jump to the given label.
    Init { target: Label },

    /// Unconditional jump to label.
    Goto { target: Label },

    /// Store current PC in return_reg, then jump to target.
    Gosub { target: Label, return_reg: Reg },

    /// Return to address stored in return_reg.
    Return {
        return_reg: Reg,
        /// If true, fall through if return_reg doesn't contain an integer.
        can_fallthrough: bool,
    },

    /// Halt execution.
    Halt {
        err_code: usize,
        description: String,
    },

    /// Halt if the register is null.
    HaltIfNull {
        reg: Reg,
        err_code: usize,
        description: String,
    },

    /// Execute only once; jump to target on re-entry.
    Once { target: Label },

    // =========================================================================
    // Comparison and Branching
    // =========================================================================
    /// Jump if lhs == rhs.
    Eq {
        lhs: Reg,
        rhs: Reg,
        target: Label,
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },

    /// Jump if lhs != rhs.
    Ne {
        lhs: Reg,
        rhs: Reg,
        target: Label,
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },

    /// Jump if lhs < rhs.
    Lt {
        lhs: Reg,
        rhs: Reg,
        target: Label,
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },

    /// Jump if lhs <= rhs.
    Le {
        lhs: Reg,
        rhs: Reg,
        target: Label,
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },

    /// Jump if lhs > rhs.
    Gt {
        lhs: Reg,
        rhs: Reg,
        target: Label,
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },

    /// Jump if lhs >= rhs.
    Ge {
        lhs: Reg,
        rhs: Reg,
        target: Label,
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },

    /// Jump if reg != 0 (or null with jump_if_null).
    If {
        reg: Reg,
        target: Label,
        jump_if_null: bool,
    },

    /// Jump if reg == 0 (or null with jump_if_null).
    IfNot {
        reg: Reg,
        target: Label,
        jump_if_null: bool,
    },

    /// Jump if reg is NULL.
    IsNull { reg: Reg, target: Label },

    /// Jump if reg is not NULL.
    NotNull { reg: Reg, target: Label },

    /// Jump if reg > 0; decrement reg by amount.
    IfPos {
        reg: Reg,
        target: Label,
        decrement_by: usize,
    },

    /// Compare two register vectors, setting comparison state.
    Compare {
        start_reg_a: Reg,
        start_reg_b: Reg,
        count: usize,
    },

    /// Jump based on last Compare result.
    Jump {
        target_lt: Label,
        target_eq: Label,
        target_gt: Label,
    },

    // =========================================================================
    // Cursor Operations
    // =========================================================================
    /// Open a read cursor on a table.
    OpenRead {
        cursor: Cursor,
        root_page: PageIdx,
        db: usize,
    },

    /// Open a write cursor on a table.
    OpenWrite {
        cursor: Cursor,
        root_page: PageIdx,
        db: usize,
    },

    /// Open a pseudo cursor (reads from a register).
    OpenPseudo {
        cursor: Cursor,
        content_reg: Reg,
        num_fields: usize,
    },

    /// Open an ephemeral table.
    OpenEphemeral { cursor: Cursor, is_table: bool },

    /// Close a cursor.
    Close { cursor: Cursor },

    /// Rewind cursor to first row; jump if empty.
    Rewind { cursor: Cursor, if_empty: Label },

    /// Move cursor to last row; jump if empty.
    Last { cursor: Cursor, if_empty: Label },

    /// Advance to next row; jump if there is one.
    Next { cursor: Cursor, if_next: Label },

    /// Move to previous row; jump if there is one.
    Prev { cursor: Cursor, if_prev: Label },

    /// Seek to a specific rowid; jump if not found.
    SeekRowid {
        cursor: Cursor,
        rowid_reg: Reg,
        if_not_found: Label,
    },

    /// Seek to end of cursor (for appending).
    SeekEnd { cursor: Cursor },

    /// Move cursor to null row.
    NullRow { cursor: Cursor },

    /// Deferred seek from index cursor to table cursor.
    DeferredSeek {
        index_cursor: Cursor,
        table_cursor: Cursor,
    },

    // =========================================================================
    // Index Operations
    // =========================================================================
    /// Position index cursor at first entry >= key.
    SeekGe {
        cursor: Cursor,
        key_reg: Reg,
        num_keys: usize,
        if_not_found: Label,
    },

    /// Position index cursor at first entry > key.
    SeekGt {
        cursor: Cursor,
        key_reg: Reg,
        num_keys: usize,
        if_not_found: Label,
    },

    /// Position index cursor at last entry <= key.
    SeekLe {
        cursor: Cursor,
        key_reg: Reg,
        num_keys: usize,
        if_not_found: Label,
    },

    /// Position index cursor at last entry < key.
    SeekLt {
        cursor: Cursor,
        key_reg: Reg,
        num_keys: usize,
        if_not_found: Label,
    },

    /// Jump if index key > registers.
    IdxGt {
        cursor: Cursor,
        key_reg: Reg,
        num_keys: usize,
        target: Label,
    },

    /// Jump if index key >= registers.
    IdxGe {
        cursor: Cursor,
        key_reg: Reg,
        num_keys: usize,
        target: Label,
    },

    /// Jump if index key < registers.
    IdxLt {
        cursor: Cursor,
        key_reg: Reg,
        num_keys: usize,
        target: Label,
    },

    /// Jump if index key <= registers.
    IdxLe {
        cursor: Cursor,
        key_reg: Reg,
        num_keys: usize,
        target: Label,
    },

    /// Insert into index.
    IdxInsert {
        cursor: Cursor,
        key_reg: Reg,
        flags: IdxInsertFlags,
    },

    /// Get rowid from index cursor.
    IdxRowId { cursor: Cursor, dest: Reg },

    // =========================================================================
    // Data Access
    // =========================================================================
    /// Read a column from cursor.
    Column {
        cursor: Cursor,
        column: usize,
        dest: Reg,
    },

    /// Read the rowid from cursor.
    RowId { cursor: Cursor, dest: Reg },

    /// Read the entire row data.
    RowData { cursor: Cursor, dest: Reg },

    // =========================================================================
    // Data Manipulation
    // =========================================================================
    /// Copy registers.
    Copy { src: Reg, dest: Reg, count: usize },

    /// Shallow copy (single register).
    SCopy { src: Reg, dest: Reg },

    /// Move registers (source becomes NULL).
    Move { src: Reg, dest: Reg, count: usize },

    // =========================================================================
    // Value Loading
    // =========================================================================
    /// Set register(s) to NULL.
    Null { dest: Reg, count: usize },

    /// Load integer constant.
    Integer { value: i64, dest: Reg },

    /// Load float constant.
    Real { value: f64, dest: Reg },

    /// Load string constant.
    String8 { value: String, dest: Reg },

    /// Load blob constant.
    Blob { value: Vec<u8>, dest: Reg },

    /// Convert integer to real if applicable.
    RealAffinity { reg: Reg },

    // =========================================================================
    // Arithmetic
    // =========================================================================
    /// Add two registers.
    Add { lhs: Reg, rhs: Reg, dest: Reg },

    /// Subtract rhs from lhs.
    Subtract { lhs: Reg, rhs: Reg, dest: Reg },

    /// Multiply two registers.
    Multiply { lhs: Reg, rhs: Reg, dest: Reg },

    /// Divide lhs by rhs.
    Divide { lhs: Reg, rhs: Reg, dest: Reg },

    /// Remainder of lhs / rhs.
    Remainder { lhs: Reg, rhs: Reg, dest: Reg },

    /// Bitwise AND.
    BitAnd { lhs: Reg, rhs: Reg, dest: Reg },

    /// Bitwise OR.
    BitOr { lhs: Reg, rhs: Reg, dest: Reg },

    /// Bitwise NOT.
    BitNot { reg: Reg, dest: Reg },

    /// Negate value.
    Negative { reg: Reg, dest: Reg },

    // =========================================================================
    // Record Operations
    // =========================================================================
    /// Create a record from registers.
    MakeRecord {
        start_reg: Reg,
        count: usize,
        dest: Reg,
    },

    /// Emit a result row.
    ResultRow { start_reg: Reg, count: usize },

    // =========================================================================
    // Insert/Update/Delete
    // =========================================================================
    /// Generate a new rowid.
    NewRowId { cursor: Cursor, dest: Reg },

    /// Insert a row.
    Insert {
        cursor: Cursor,
        key_reg: Reg,
        record_reg: Reg,
        flags: InsertFlags,
    },

    /// Delete current row.
    Delete { cursor: Cursor },

    // =========================================================================
    // Aggregation
    // =========================================================================
    /// Initialize aggregate accumulator.
    AggInit { dest: Reg, func: AggFunc },

    /// Aggregate step.
    AggStep {
        func: AggFunc,
        args_start: Reg,
        arg_count: usize,
        dest: Reg,
    },

    /// Finalize aggregate.
    AggFinal { dest: Reg, func: AggFunc },

    /// Like AggFinal but keeps accumulator.
    AggValue { dest: Reg, func: AggFunc },

    // =========================================================================
    // Sorter Operations
    // =========================================================================
    /// Open a sorter cursor.
    SorterOpen { cursor: Cursor, num_columns: usize },

    /// Insert into sorter.
    SorterInsert { cursor: Cursor, record_reg: Reg },

    /// Sort the sorter data.
    SorterSort { cursor: Cursor, if_empty: Label },

    /// Read data from sorter.
    SorterData {
        cursor: Cursor,
        dest: Reg,
        /// Pseudo cursor to write the data to.
        pseudo_cursor: Option<Cursor>,
    },

    /// Move to next sorter row.
    SorterNext { cursor: Cursor, if_next: Label },

    // =========================================================================
    // Subroutine Support
    // =========================================================================
    /// Begin a subroutine (like Null but semantic).
    BeginSubrtn { dest: Reg },

    /// End a coroutine.
    EndCoroutine { yield_reg: Reg },

    /// Initialize a coroutine.
    InitCoroutine {
        yield_reg: Reg,
        jump_on_init: Label,
        start_label: Label,
    },

    /// Yield from a coroutine.
    Yield { yield_reg: Reg, resume_label: Label },

    // =========================================================================
    // Transaction Control
    // =========================================================================
    /// Start a transaction.
    Transaction { db: usize, write: bool },

    /// Set auto-commit mode.
    AutoCommit { auto_commit: bool, rollback: bool },

    // =========================================================================
    // Scalar Functions
    // =========================================================================
    /// Call a scalar function.
    Function {
        func_name: String,
        args_start: Reg,
        arg_count: usize,
        dest: Reg,
    },

    // =========================================================================
    // Miscellaneous
    // =========================================================================
    /// No operation.
    Noop,

    /// Decrement register and jump if not zero.
    DecrJumpZero { reg: Reg, target: Label },

    /// Copy affinity string for a table.
    Affinity { start_reg: Reg, affinity: String },

    /// Check types match table schema.
    TypeCheck {
        start_reg: Reg,
        count: usize,
        table: Arc<BTreeTable>,
    },

    /// Trace message (for debugging).
    Trace { message: String },
}

impl InsnSpec {
    /// Check if this instruction is a jump/branch instruction.
    pub fn is_jump(&self) -> bool {
        matches!(
            self,
            InsnSpec::Init { .. }
                | InsnSpec::Goto { .. }
                | InsnSpec::Gosub { .. }
                | InsnSpec::Return { .. }
                | InsnSpec::Eq { .. }
                | InsnSpec::Ne { .. }
                | InsnSpec::Lt { .. }
                | InsnSpec::Le { .. }
                | InsnSpec::Gt { .. }
                | InsnSpec::Ge { .. }
                | InsnSpec::If { .. }
                | InsnSpec::IfNot { .. }
                | InsnSpec::IsNull { .. }
                | InsnSpec::NotNull { .. }
                | InsnSpec::IfPos { .. }
                | InsnSpec::Jump { .. }
                | InsnSpec::Rewind { .. }
                | InsnSpec::Last { .. }
                | InsnSpec::Next { .. }
                | InsnSpec::Prev { .. }
                | InsnSpec::SeekRowid { .. }
                | InsnSpec::SeekGe { .. }
                | InsnSpec::SeekGt { .. }
                | InsnSpec::SeekLe { .. }
                | InsnSpec::SeekLt { .. }
                | InsnSpec::IdxGt { .. }
                | InsnSpec::IdxGe { .. }
                | InsnSpec::IdxLt { .. }
                | InsnSpec::IdxLe { .. }
                | InsnSpec::SorterSort { .. }
                | InsnSpec::SorterNext { .. }
                | InsnSpec::Once { .. }
                | InsnSpec::DecrJumpZero { .. }
                | InsnSpec::Yield { .. }
        )
    }

    /// Get the labels referenced by this instruction.
    pub fn referenced_labels(&self) -> Vec<Label> {
        match self {
            InsnSpec::Init { target } => vec![*target],
            InsnSpec::Goto { target } => vec![*target],
            InsnSpec::Gosub { target, .. } => vec![*target],
            InsnSpec::Once { target } => vec![*target],
            InsnSpec::Eq { target, .. } => vec![*target],
            InsnSpec::Ne { target, .. } => vec![*target],
            InsnSpec::Lt { target, .. } => vec![*target],
            InsnSpec::Le { target, .. } => vec![*target],
            InsnSpec::Gt { target, .. } => vec![*target],
            InsnSpec::Ge { target, .. } => vec![*target],
            InsnSpec::If { target, .. } => vec![*target],
            InsnSpec::IfNot { target, .. } => vec![*target],
            InsnSpec::IsNull { target, .. } => vec![*target],
            InsnSpec::NotNull { target, .. } => vec![*target],
            InsnSpec::IfPos { target, .. } => vec![*target],
            InsnSpec::Jump {
                target_lt,
                target_eq,
                target_gt,
            } => vec![*target_lt, *target_eq, *target_gt],
            InsnSpec::Rewind { if_empty, .. } => vec![*if_empty],
            InsnSpec::Last { if_empty, .. } => vec![*if_empty],
            InsnSpec::Next { if_next, .. } => vec![*if_next],
            InsnSpec::Prev { if_prev, .. } => vec![*if_prev],
            InsnSpec::SeekRowid { if_not_found, .. } => vec![*if_not_found],
            InsnSpec::SeekGe { if_not_found, .. } => vec![*if_not_found],
            InsnSpec::SeekGt { if_not_found, .. } => vec![*if_not_found],
            InsnSpec::SeekLe { if_not_found, .. } => vec![*if_not_found],
            InsnSpec::SeekLt { if_not_found, .. } => vec![*if_not_found],
            InsnSpec::IdxGt { target, .. } => vec![*target],
            InsnSpec::IdxGe { target, .. } => vec![*target],
            InsnSpec::IdxLt { target, .. } => vec![*target],
            InsnSpec::IdxLe { target, .. } => vec![*target],
            InsnSpec::SorterSort { if_empty, .. } => vec![*if_empty],
            InsnSpec::SorterNext { if_next, .. } => vec![*if_next],
            InsnSpec::DecrJumpZero { target, .. } => vec![*target],
            InsnSpec::InitCoroutine {
                jump_on_init,
                start_label,
                ..
            } => vec![*jump_on_init, *start_label],
            InsnSpec::Yield { resume_label, .. } => vec![*resume_label],
            _ => vec![],
        }
    }

    /// Get the registers read by this instruction.
    pub fn reads_registers(&self) -> Vec<Reg> {
        match self {
            InsnSpec::Eq { lhs, rhs, .. }
            | InsnSpec::Ne { lhs, rhs, .. }
            | InsnSpec::Lt { lhs, rhs, .. }
            | InsnSpec::Le { lhs, rhs, .. }
            | InsnSpec::Gt { lhs, rhs, .. }
            | InsnSpec::Ge { lhs, rhs, .. }
            | InsnSpec::Add { lhs, rhs, .. }
            | InsnSpec::Subtract { lhs, rhs, .. }
            | InsnSpec::Multiply { lhs, rhs, .. }
            | InsnSpec::Divide { lhs, rhs, .. }
            | InsnSpec::Remainder { lhs, rhs, .. }
            | InsnSpec::BitAnd { lhs, rhs, .. }
            | InsnSpec::BitOr { lhs, rhs, .. } => vec![*lhs, *rhs],

            InsnSpec::If { reg, .. }
            | InsnSpec::IfNot { reg, .. }
            | InsnSpec::IsNull { reg, .. }
            | InsnSpec::NotNull { reg, .. }
            | InsnSpec::IfPos { reg, .. }
            | InsnSpec::BitNot { reg, .. }
            | InsnSpec::Negative { reg, .. }
            | InsnSpec::RealAffinity { reg }
            | InsnSpec::HaltIfNull { reg, .. }
            | InsnSpec::DecrJumpZero { reg, .. } => vec![*reg],

            InsnSpec::Copy { src, .. }
            | InsnSpec::SCopy { src, .. }
            | InsnSpec::Move { src, .. } => {
                vec![*src]
            }

            InsnSpec::SeekRowid { rowid_reg, .. } => vec![*rowid_reg],

            InsnSpec::Insert {
                key_reg,
                record_reg,
                ..
            } => vec![*key_reg, *record_reg],

            InsnSpec::SorterInsert { record_reg, .. } => vec![*record_reg],

            InsnSpec::MakeRecord {
                start_reg, count, ..
            } => (0..*count).map(|i| Reg(start_reg.0 + i)).collect(),

            InsnSpec::ResultRow { start_reg, count } => {
                (0..*count).map(|i| Reg(start_reg.0 + i)).collect()
            }

            _ => vec![],
        }
    }

    /// Get the registers written by this instruction.
    pub fn writes_registers(&self) -> Vec<Reg> {
        match self {
            InsnSpec::Null { dest, count } => (0..*count).map(|i| Reg(dest.0 + i)).collect(),

            InsnSpec::Integer { dest, .. }
            | InsnSpec::Real { dest, .. }
            | InsnSpec::String8 { dest, .. }
            | InsnSpec::Blob { dest, .. }
            | InsnSpec::Column { dest, .. }
            | InsnSpec::RowId { dest, .. }
            | InsnSpec::RowData { dest, .. }
            | InsnSpec::IdxRowId { dest, .. }
            | InsnSpec::NewRowId { dest, .. }
            | InsnSpec::SorterData { dest, .. }
            | InsnSpec::BeginSubrtn { dest }
            | InsnSpec::MakeRecord { dest, .. }
            | InsnSpec::AggFinal { dest, .. }
            | InsnSpec::AggValue { dest, .. }
            | InsnSpec::Function { dest, .. } => vec![*dest],

            InsnSpec::Add { dest, .. }
            | InsnSpec::Subtract { dest, .. }
            | InsnSpec::Multiply { dest, .. }
            | InsnSpec::Divide { dest, .. }
            | InsnSpec::Remainder { dest, .. }
            | InsnSpec::BitAnd { dest, .. }
            | InsnSpec::BitOr { dest, .. }
            | InsnSpec::BitNot { dest, .. }
            | InsnSpec::Negative { dest, .. } => vec![*dest],

            InsnSpec::Copy { dest, count, .. } => (0..*count).map(|i| Reg(dest.0 + i)).collect(),

            InsnSpec::SCopy { dest, .. } => vec![*dest],

            InsnSpec::Move { dest, count, .. } => (0..*count).map(|i| Reg(dest.0 + i)).collect(),

            InsnSpec::Gosub { return_reg, .. } => vec![*return_reg],

            _ => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_jump() {
        let goto = InsnSpec::Goto { target: Label(0) };
        assert!(goto.is_jump());

        let integer = InsnSpec::Integer {
            value: 42,
            dest: Reg(0),
        };
        assert!(!integer.is_jump());
    }

    #[test]
    fn test_referenced_labels() {
        let jump = InsnSpec::Jump {
            target_lt: Label(0),
            target_eq: Label(1),
            target_gt: Label(2),
        };
        let labels = jump.referenced_labels();
        assert_eq!(labels.len(), 3);

        let noop = InsnSpec::Noop;
        assert!(noop.referenced_labels().is_empty());
    }

    #[test]
    fn test_reads_writes() {
        let add = InsnSpec::Add {
            lhs: Reg(1),
            rhs: Reg(2),
            dest: Reg(3),
        };

        let reads = add.reads_registers();
        assert_eq!(reads.len(), 2);
        assert!(reads.contains(&Reg(1)));
        assert!(reads.contains(&Reg(2)));

        let writes = add.writes_registers();
        assert_eq!(writes.len(), 1);
        assert!(writes.contains(&Reg(3)));
    }
}
