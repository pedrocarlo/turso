use limbo_sqlite3_parser::ast::SortOrder;

use crate::vdbe::{builder::CursorType, insn::RegisterOrLiteral};

use super::{Insn, InsnReference, OwnedValue, Program};
use crate::function::{Func, ScalarFunc};
use std::rc::Rc;

pub struct ExplainRow {
    addr: InsnReference,
    opcode: &'static str,
    p1: i32,
    p2: i32,
    p3: i32,
    p4: OwnedValue,
    p5: u16,
    comment: String,
}

pub fn insn_to_explain_row(program: &Program, addr: InsnReference, insn: &Insn) -> ExplainRow {
    match insn {
        Insn::Init { target_pc } => ExplainRow {
            addr,
            opcode: "Init",
            p1: 0,
            p2: target_pc.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("Start at {}", target_pc.to_debug_int()),
        },
        Insn::Add { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "Add",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}]+r[{}]", dest, lhs, rhs),
        },
        Insn::Subtract { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "Subtract",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}]-r[{}]", dest, lhs, rhs),
        },
        Insn::Multiply { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "Multiply",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}]*r[{}]", dest, lhs, rhs),
        },
        Insn::Divide { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "Divide",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}]/r[{}]", dest, lhs, rhs),
        },
        Insn::BitAnd { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "BitAnd",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}]&r[{}]", dest, lhs, rhs),
        },
        Insn::BitOr { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "BitOr",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}]|r[{}]", dest, lhs, rhs),
        },
        Insn::BitNot { reg, dest } => ExplainRow {
            addr,
            opcode: "BitNot",
            p1: *reg as i32,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=~r[{}]", dest, reg),
        },
        Insn::Checkpoint {
            database,
            checkpoint_mode: _,
            dest,
        } => ExplainRow {
            addr,
            opcode: "Checkpoint",
            p1: *database as i32,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=~r[{}]", dest, database),
        },
        Insn::Remainder { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "Remainder",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}]%r[{}]", dest, lhs, rhs),
        },
        Insn::Null { dest, dest_end } => ExplainRow {
            addr,
            opcode: "Null",
            p1: 0,
            p2: *dest as i32,
            p3: dest_end.map_or(0, |end| end as i32),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: dest_end.map_or(format!("r[{}]=NULL", dest), |end| {
                format!("r[{}..{}]=NULL", dest, end)
            }),
        },
        Insn::NullRow { cursor_id } => ExplainRow {
            addr,
            opcode: "NullRow",
            p1: *cursor_id as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("Set cursor {} to a (pseudo) NULL row", cursor_id),
        },
        Insn::NotNull { reg, target_pc } => ExplainRow {
            addr,
            opcode: "NotNull",
            p1: *reg as i32,
            p2: target_pc.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]!=NULL -> goto {}", reg, target_pc.to_debug_int()),
        },
        Insn::Compare {
            start_reg_a,
            start_reg_b,
            count,
        } => ExplainRow {
            addr,
            opcode: "Compare",
            p1: *start_reg_a as i32,
            p2: *start_reg_b as i32,
            p3: *count as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "r[{}..{}]==r[{}..{}]",
                start_reg_a,
                start_reg_a + (count - 1),
                start_reg_b,
                start_reg_b + (count - 1)
            ),
        },
        Insn::Jump {
            target_pc_lt,
            target_pc_eq,
            target_pc_gt,
        } => ExplainRow {
            addr,
            opcode: "Jump",
            p1: target_pc_lt.to_debug_int(),
            p2: target_pc_eq.to_debug_int(),
            p3: target_pc_gt.to_debug_int(),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Move {
            source_reg,
            dest_reg,
            count,
        } => ExplainRow {
            addr,
            opcode: "Move",
            p1: *source_reg as i32,
            p2: *dest_reg as i32,
            p3: *count as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "r[{}..{}]=r[{}..{}]",
                dest_reg,
                dest_reg + (count - 1),
                source_reg,
                source_reg + (count - 1)
            ),
        },
        Insn::IfPos {
            reg,
            target_pc,
            decrement_by,
        } => ExplainRow {
            addr,
            opcode: "IfPos",
            p1: *reg as i32,
            p2: target_pc.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "r[{}]>0 -> r[{}]-={}, goto {}",
                reg,
                reg,
                decrement_by,
                target_pc.to_debug_int()
            ),
        },
        Insn::Eq {
            lhs,
            rhs,
            target_pc,
            ..
        } => ExplainRow {
            addr,
            opcode: "Eq",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: target_pc.to_debug_int(),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "if r[{}]==r[{}] goto {}",
                lhs,
                rhs,
                target_pc.to_debug_int()
            ),
        },
        Insn::Ne {
            lhs,
            rhs,
            target_pc,
            ..
        } => ExplainRow {
            addr,
            opcode: "Ne",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: target_pc.to_debug_int(),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "if r[{}]!=r[{}] goto {}",
                lhs,
                rhs,
                target_pc.to_debug_int()
            ),
        },
        Insn::Lt {
            lhs,
            rhs,
            target_pc,
            ..
        } => ExplainRow {
            addr,
            opcode: "Lt",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: target_pc.to_debug_int(),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("if r[{}]<r[{}] goto {}", lhs, rhs, target_pc.to_debug_int()),
        },
        Insn::Le {
            lhs,
            rhs,
            target_pc,
            ..
        } => ExplainRow {
            addr,
            opcode: "Le",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: target_pc.to_debug_int(),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "if r[{}]<=r[{}] goto {}",
                lhs,
                rhs,
                target_pc.to_debug_int()
            ),
        },
        Insn::Gt {
            lhs,
            rhs,
            target_pc,
            ..
        } => ExplainRow {
            addr,
            opcode: "Gt",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: target_pc.to_debug_int(),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("if r[{}]>r[{}] goto {}", lhs, rhs, target_pc.to_debug_int()),
        },
        Insn::Ge {
            lhs,
            rhs,
            target_pc,
            ..
        } => ExplainRow {
            addr,
            opcode: "Ge",
            p1: *lhs as i32,
            p2: *rhs as i32,
            p3: target_pc.to_debug_int(),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "if r[{}]>=r[{}] goto {}",
                lhs,
                rhs,
                target_pc.to_debug_int()
            ),
        },
        Insn::If {
            reg,
            target_pc,
            jump_if_null,
        } => ExplainRow {
            addr,
            opcode: "If",
            p1: *reg as i32,
            p2: target_pc.to_debug_int(),
            p3: *jump_if_null as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("if r[{}] goto {}", reg, target_pc.to_debug_int()),
        },
        Insn::IfNot {
            reg,
            target_pc,
            jump_if_null,
        } => ExplainRow {
            addr,
            opcode: "IfNot",
            p1: *reg as i32,
            p2: target_pc.to_debug_int(),
            p3: *jump_if_null as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("if !r[{}] goto {}", reg, target_pc.to_debug_int()),
        },
        Insn::OpenRead {
            cursor_id,
            root_page,
        } => ExplainRow {
            addr,
            opcode: "OpenRead",
            p1: *cursor_id as i32,
            p2: *root_page as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "table={}, root={}",
                program.cursor_ref[*cursor_id]
                    .0
                    .as_ref()
                    .unwrap_or(&format!("cursor {}", cursor_id)),
                root_page
            ),
        },
        Insn::VOpen { cursor_id } => ExplainRow {
            addr,
            opcode: "VOpen",
            p1: *cursor_id as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::VCreate {
            table_name,
            module_name,
            args_reg,
        } => ExplainRow {
            addr,
            opcode: "VCreate",
            p1: *table_name as i32,
            p2: *module_name as i32,
            p3: args_reg.unwrap_or(0) as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("table={}, module={}", table_name, module_name),
        },
        Insn::VFilter {
            cursor_id,
            pc_if_empty,
            arg_count,
            ..
        } => ExplainRow {
            addr,
            opcode: "VFilter",
            p1: *cursor_id as i32,
            p2: pc_if_empty.to_debug_int(),
            p3: *arg_count as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::VColumn {
            cursor_id,
            column,
            dest,
        } => ExplainRow {
            addr,
            opcode: "VColumn",
            p1: *cursor_id as i32,
            p2: *column as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::VUpdate {
            cursor_id,
            arg_count,       // P2: Number of arguments in argv[]
            start_reg,       // P3: Start register for argv[]
            vtab_ptr,        // P4: vtab pointer
            conflict_action, // P5: Conflict resolution flags
        } => ExplainRow {
            addr,
            opcode: "VUpdate",
            p1: *cursor_id as i32,
            p2: *arg_count as i32,
            p3: *start_reg as i32,
            p4: OwnedValue::build_text(&format!("vtab:{}", vtab_ptr)),
            p5: *conflict_action,
            comment: format!("args=r[{}..{}]", start_reg, start_reg + arg_count - 1),
        },
        Insn::VNext {
            cursor_id,
            pc_if_next,
        } => ExplainRow {
            addr,
            opcode: "VNext",
            p1: *cursor_id as i32,
            p2: pc_if_next.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::VDestroy { db, table_name } => ExplainRow {
            addr,
            opcode: "VDestroy",
            p1: *db as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(table_name),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::OpenPseudo {
            cursor_id,
            content_reg,
            num_fields,
        } => ExplainRow {
            addr,
            opcode: "OpenPseudo",
            p1: *cursor_id as i32,
            p2: *content_reg as i32,
            p3: *num_fields as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("{} columns in r[{}]", num_fields, content_reg),
        },
        Insn::Rewind {
            cursor_id,
            pc_if_empty,
        } => ExplainRow {
            addr,
            opcode: "Rewind",
            p1: *cursor_id as i32,
            p2: pc_if_empty.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "Rewind {}",
                program.cursor_ref[*cursor_id]
                    .0
                    .as_ref()
                    .unwrap_or(&format!("cursor {}", cursor_id))
            ),
        },
        Insn::Column {
            cursor_id,
            column,
            dest,
        } => {
            let (table_identifier, cursor_type) = &program.cursor_ref[*cursor_id];
            let column_name: Option<&String> = match cursor_type {
                CursorType::BTreeTable(table) => {
                    let name = table.columns.get(*column).unwrap().name.as_ref();
                    name
                }
                CursorType::BTreeIndex(index) => {
                    let name = &index.columns.get(*column).unwrap().name;
                    Some(name)
                }
                CursorType::Pseudo(pseudo_table) => {
                    let name = pseudo_table.columns.get(*column).unwrap().name.as_ref();
                    name
                }
                CursorType::Sorter => None,
                CursorType::VirtualTable(v) => v.columns.get(*column).unwrap().name.as_ref(),
            };
            ExplainRow {
                addr,
                opcode: "Column",
                p1: *cursor_id as i32,
                p2: *column as i32,
                p3: *dest as i32,
                p4: OwnedValue::build_text(""),
                p5: 0,
                comment: format!(
                    "r[{}]={}.{}",
                    dest,
                    table_identifier
                        .as_ref()
                        .unwrap_or(&format!("cursor {}", cursor_id)),
                    column_name.unwrap_or(&format!("column {}", *column))
                ),
            }
        }
        Insn::TypeCheck {
            start_reg,
            count,
            check_generated,
            ..
        } => ExplainRow {
            addr,
            opcode: "TypeCheck",
            p1: *start_reg as i32,
            p2: *count as i32,
            p3: *check_generated as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: String::from(""),
        },
        Insn::MakeRecord {
            start_reg,
            count,
            dest_reg,
        } => ExplainRow {
            addr,
            opcode: "MakeRecord",
            p1: *start_reg as i32,
            p2: *count as i32,
            p3: *dest_reg as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "r[{}]=mkrec(r[{}..{}])",
                dest_reg,
                start_reg,
                start_reg + count - 1,
            ),
        },
        Insn::ResultRow { start_reg, count } => ExplainRow {
            addr,
            opcode: "ResultRow",
            p1: *start_reg as i32,
            p2: *count as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: if *count == 1 {
                format!("output=r[{}]", start_reg)
            } else {
                format!("output=r[{}..{}]", start_reg, start_reg + count - 1)
            },
        },
        Insn::Next {
            cursor_id,
            pc_if_next,
        } => ExplainRow {
            addr,
            opcode: "Next",
            p1: *cursor_id as i32,
            p2: pc_if_next.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Halt {
            err_code,
            description,
        } => ExplainRow {
            addr,
            opcode: "Halt",
            p1: *err_code as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(&description),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Transaction { write } => ExplainRow {
            addr,
            opcode: "Transaction",
            p1: 0,
            p2: *write as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("write={}", write),
        },
        Insn::Goto { target_pc } => ExplainRow {
            addr,
            opcode: "Goto",
            p1: 0,
            p2: target_pc.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Gosub {
            target_pc,
            return_reg,
        } => ExplainRow {
            addr,
            opcode: "Gosub",
            p1: *return_reg as i32,
            p2: target_pc.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Return { return_reg } => ExplainRow {
            addr,
            opcode: "Return",
            p1: *return_reg as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Integer { value, dest } => ExplainRow {
            addr,
            opcode: "Integer",
            p1: *value as i32,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]={}", dest, value),
        },
        Insn::Real { value, dest } => ExplainRow {
            addr,
            opcode: "Real",
            p1: 0,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::Float(*value),
            p5: 0,
            comment: format!("r[{}]={}", dest, value),
        },
        Insn::RealAffinity { register } => ExplainRow {
            addr,
            opcode: "RealAffinity",
            p1: *register as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::String8 { value, dest } => ExplainRow {
            addr,
            opcode: "String8",
            p1: 0,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::build_text(value),
            p5: 0,
            comment: format!("r[{}]='{}'", dest, value),
        },
        Insn::Blob { value, dest } => ExplainRow {
            addr,
            opcode: "Blob",
            p1: 0,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::Blob(value.clone()),
            p5: 0,
            comment: format!(
                "r[{}]={} (len={})",
                dest,
                String::from_utf8_lossy(value),
                value.len()
            ),
        },
        Insn::RowId { cursor_id, dest } => ExplainRow {
            addr,
            opcode: "RowId",
            p1: *cursor_id as i32,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "r[{}]={}.rowid",
                dest,
                &program.cursor_ref[*cursor_id]
                    .0
                    .as_ref()
                    .unwrap_or(&format!("cursor {}", cursor_id))
            ),
        },
        Insn::IdxRowId { cursor_id, dest } => ExplainRow {
            addr,
            opcode: "IdxRowId",
            p1: *cursor_id as i32,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "r[{}]={}.rowid",
                dest,
                &program.cursor_ref[*cursor_id]
                    .0
                    .as_ref()
                    .unwrap_or(&format!("cursor {}", cursor_id))
            ),
        },
        Insn::SeekRowid {
            cursor_id,
            src_reg,
            target_pc,
        } => ExplainRow {
            addr,
            opcode: "SeekRowid",
            p1: *cursor_id as i32,
            p2: *src_reg as i32,
            p3: target_pc.to_debug_int(),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "if (r[{}]!={}.rowid) goto {}",
                src_reg,
                &program.cursor_ref[*cursor_id]
                    .0
                    .as_ref()
                    .unwrap_or(&format!("cursor {}", cursor_id)),
                target_pc.to_debug_int()
            ),
        },
        Insn::DeferredSeek {
            index_cursor_id,
            table_cursor_id,
        } => ExplainRow {
            addr,
            opcode: "DeferredSeek",
            p1: *index_cursor_id as i32,
            p2: *table_cursor_id as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::SeekGT {
            is_index: _,
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        }
        | Insn::SeekGE {
            is_index: _,
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        }
        | Insn::SeekLE {
            is_index: _,
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        }
        | Insn::SeekLT {
            is_index: _,
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        } => ExplainRow {
            addr,
            opcode: match insn {
                Insn::SeekGT { .. } => "SeekGT",
                Insn::SeekGE { .. } => "SeekGE",
                Insn::SeekLE { .. } => "SeekLE",
                Insn::SeekLT { .. } => "SeekLT",
                _ => unreachable!(),
            },
            p1: *cursor_id as i32,
            p2: target_pc.to_debug_int(),
            p3: *start_reg as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("key=[{}..{}]", start_reg, start_reg + num_regs - 1),
        },
        Insn::SeekEnd { cursor_id } => ExplainRow {
            addr,
            opcode: "SeekEnd",
            p1: *cursor_id as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::IdxInsert {
            cursor_id,
            record_reg,
            unpacked_start,
            flags,
            ..
        } => ExplainRow {
            addr,
            opcode: "IdxInsert",
            p1: *cursor_id as i32,
            p2: *record_reg as i32,
            p3: unpacked_start.unwrap_or(0) as i32,
            p4: OwnedValue::build_text(""),
            p5: flags.0 as u16,
            comment: format!("key=r[{}]", record_reg),
        },
        Insn::IdxGT {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        }
        | Insn::IdxGE {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        }
        | Insn::IdxLE {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        }
        | Insn::IdxLT {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        } => ExplainRow {
            addr,
            opcode: match insn {
                Insn::IdxGT { .. } => "IdxGT",
                Insn::IdxGE { .. } => "IdxGE",
                Insn::IdxLE { .. } => "IdxLE",
                Insn::IdxLT { .. } => "IdxLT",
                _ => unreachable!(),
            },
            p1: *cursor_id as i32,
            p2: target_pc.to_debug_int(),
            p3: *start_reg as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("key=[{}..{}]", start_reg, start_reg + num_regs - 1),
        },
        Insn::DecrJumpZero { reg, target_pc } => ExplainRow {
            addr,
            opcode: "DecrJumpZero",
            p1: *reg as i32,
            p2: target_pc.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("if (--r[{}]==0) goto {}", reg, target_pc.to_debug_int()),
        },
        Insn::AggStep {
            func,
            acc_reg,
            delimiter: _,
            col,
        } => ExplainRow {
            addr,
            opcode: "AggStep",
            p1: 0,
            p2: *col as i32,
            p3: *acc_reg as i32,
            p4: OwnedValue::build_text(func.to_string()),
            p5: 0,
            comment: format!("accum=r[{}] step(r[{}])", *acc_reg, *col),
        },
        Insn::AggFinal { register, func } => ExplainRow {
            addr,
            opcode: "AggFinal",
            p1: 0,
            p2: *register as i32,
            p3: 0,
            p4: OwnedValue::build_text(func.to_string()),
            p5: 0,
            comment: format!("accum=r[{}]", *register),
        },
        Insn::SorterOpen {
            cursor_id,
            columns,
            order,
        } => {
            let _p4 = String::new();
            let to_print: Vec<String> = order
                .iter()
                .map(|v| match v {
                    SortOrder::Asc => "B".to_string(),
                    SortOrder::Desc => "-B".to_string(),
                })
                .collect();

            ExplainRow {
                addr,
                opcode: "SorterOpen",
                p1: *cursor_id as i32,
                p2: *columns as i32,
                p3: 0,
                p4: OwnedValue::build_text(&(format!("k({},{})", order.len(), to_print.join(",")))),
                p5: 0,
                comment: format!("cursor={}", cursor_id),
            }
        }
        Insn::SorterData {
            cursor_id,
            dest_reg,
            pseudo_cursor,
        } => ExplainRow {
            addr,
            opcode: "SorterData",
            p1: *cursor_id as i32,
            p2: *dest_reg as i32,
            p3: *pseudo_cursor as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=data", dest_reg),
        },
        Insn::SorterInsert {
            cursor_id,
            record_reg,
        } => ExplainRow {
            addr,
            opcode: "SorterInsert",
            p1: *cursor_id as i32,
            p2: *record_reg as i32,
            p3: 0,
            p4: OwnedValue::Integer(0),
            p5: 0,
            comment: format!("key=r[{}]", record_reg),
        },
        Insn::SorterSort {
            cursor_id,
            pc_if_empty,
        } => ExplainRow {
            addr,
            opcode: "SorterSort",
            p1: *cursor_id as i32,
            p2: pc_if_empty.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::SorterNext {
            cursor_id,
            pc_if_next,
        } => ExplainRow {
            addr,
            opcode: "SorterNext",
            p1: *cursor_id as i32,
            p2: pc_if_next.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Function {
            constant_mask,
            start_reg,
            dest,
            func,
        } => ExplainRow {
            addr,
            opcode: "Function",
            p1: *constant_mask,
            p2: *start_reg as i32,
            p3: *dest as i32,
            p4: {
                let s = if matches!(&func.func, Func::Scalar(ScalarFunc::Like)) {
                    format!("like({})", func.arg_count)
                } else {
                    func.func.to_string()
                };
                OwnedValue::build_text(&s)
            },
            p5: 0,
            comment: if func.arg_count == 0 {
                format!("r[{}]=func()", dest)
            } else if *start_reg == *start_reg + func.arg_count - 1 {
                format!("r[{}]=func(r[{}])", dest, start_reg)
            } else {
                format!(
                    "r[{}]=func(r[{}..{}])",
                    dest,
                    start_reg,
                    start_reg + func.arg_count - 1
                )
            },
        },
        Insn::InitCoroutine {
            yield_reg,
            jump_on_definition,
            start_offset,
        } => ExplainRow {
            addr,
            opcode: "InitCoroutine",
            p1: *yield_reg as i32,
            p2: jump_on_definition.to_debug_int(),
            p3: start_offset.to_debug_int(),
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::EndCoroutine { yield_reg } => ExplainRow {
            addr,
            opcode: "EndCoroutine",
            p1: *yield_reg as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Yield {
            yield_reg,
            end_offset,
        } => ExplainRow {
            addr,
            opcode: "Yield",
            p1: *yield_reg as i32,
            p2: end_offset.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Insert {
            cursor,
            key_reg,
            record_reg,
            flag,
        } => ExplainRow {
            addr,
            opcode: "Insert",
            p1: *cursor as i32,
            p2: *record_reg as i32,
            p3: *key_reg as i32,
            p4: OwnedValue::build_text(""),
            p5: *flag as u16,
            comment: "".to_string(),
        },
        Insn::Delete { cursor_id } => ExplainRow {
            addr,
            opcode: "Delete",
            p1: *cursor_id as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::IdxDelete {
            cursor_id,
            start_reg,
            num_regs,
        } => ExplainRow {
            addr,
            opcode: "IdxDelete",
            p1: *cursor_id as i32,
            p2: *start_reg as i32,
            p3: *num_regs as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::NewRowid {
            cursor,
            rowid_reg,
            prev_largest_reg,
        } => ExplainRow {
            addr,
            opcode: "NewRowId",
            p1: *cursor as i32,
            p2: *rowid_reg as i32,
            p3: *prev_largest_reg as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::MustBeInt { reg } => ExplainRow {
            addr,
            opcode: "MustBeInt",
            p1: *reg as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::SoftNull { reg } => ExplainRow {
            addr,
            opcode: "SoftNull",
            p1: *reg as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::NoConflict {
            cursor_id,
            target_pc,
            record_reg,
            num_regs,
        } => ExplainRow {
            addr,
            opcode: "NoConflict",
            p1: *cursor_id as i32,
            p2: target_pc.to_debug_int(),
            p3: *record_reg as i32,
            p4: OwnedValue::build_text(&format!("{num_regs}")),
            p5: 0,
            comment: format!("key=r[{}]", record_reg),
        },
        Insn::NotExists {
            cursor,
            rowid_reg,
            target_pc,
        } => ExplainRow {
            addr,
            opcode: "NotExists",
            p1: *cursor as i32,
            p2: target_pc.to_debug_int(),
            p3: *rowid_reg as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::OffsetLimit {
            limit_reg,
            combined_reg,
            offset_reg,
        } => ExplainRow {
            addr,
            opcode: "OffsetLimit",
            p1: *limit_reg as i32,
            p2: *combined_reg as i32,
            p3: *offset_reg as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "if r[{}]>0 then r[{}]=r[{}]+max(0,r[{}]) else r[{}]=(-1)",
                limit_reg, combined_reg, limit_reg, offset_reg, combined_reg
            ),
        },
        Insn::OpenWrite {
            cursor_id,
            root_page,
            ..
        } => ExplainRow {
            addr,
            opcode: "OpenWrite",
            p1: *cursor_id as i32,
            p2: match root_page {
                RegisterOrLiteral::Literal(i) => *i as _,
                RegisterOrLiteral::Register(i) => *i as _,
            },
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Copy {
            src_reg,
            dst_reg,
            amount,
        } => ExplainRow {
            addr,
            opcode: "Copy",
            p1: *src_reg as i32,
            p2: *dst_reg as i32,
            p3: *amount as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}]", dst_reg, src_reg),
        },
        Insn::CreateBtree { db, root, flags } => ExplainRow {
            addr,
            opcode: "CreateBtree",
            p1: *db as i32,
            p2: *root as i32,
            p3: flags.get_flags() as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=root iDb={} flags={}", root, db, flags.get_flags()),
        },
        Insn::Destroy {
            root,
            former_root_reg,
            is_temp,
        } => ExplainRow {
            addr,
            opcode: "Destroy",
            p1: *root as i32,
            p2: *former_root_reg as i32,
            p3: *is_temp as i32,
            p4: OwnedValue::build_text(&Rc::new("".to_string())),
            p5: 0,
            comment: format!(
                "root iDb={} former_root={} is_temp={}",
                root, former_root_reg, is_temp
            ),
        },
        Insn::DropTable {
            db,
            _p2,
            _p3,
            table_name,
        } => ExplainRow {
            addr,
            opcode: "DropTable",
            p1: *db as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(&Rc::new(table_name.clone())),
            p5: 0,
            comment: format!("DROP TABLE {}", table_name),
        },
        Insn::Close { cursor_id } => ExplainRow {
            addr,
            opcode: "Close",
            p1: *cursor_id as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::Last {
            cursor_id,
            pc_if_empty,
        } => ExplainRow {
            addr,
            opcode: "Last",
            p1: *cursor_id as i32,
            p2: pc_if_empty.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::IsNull { reg, target_pc } => ExplainRow {
            addr,
            opcode: "IsNull",
            p1: *reg as i32,
            p2: target_pc.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("if (r[{}]==NULL) goto {}", reg, target_pc.to_debug_int()),
        },
        Insn::ParseSchema { db, where_clause } => ExplainRow {
            addr,
            opcode: "ParseSchema",
            p1: *db as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(where_clause),
            p5: 0,
            comment: where_clause.clone(),
        },
        Insn::Prev {
            cursor_id,
            pc_if_prev,
        } => ExplainRow {
            addr,
            opcode: "Prev",
            p1: *cursor_id as i32,
            p2: pc_if_prev.to_debug_int(),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::ShiftRight { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "ShiftRight",
            p1: *rhs as i32,
            p2: *lhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}] >> r[{}]", dest, lhs, rhs),
        },
        Insn::ShiftLeft { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "ShiftLeft",
            p1: *rhs as i32,
            p2: *lhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}] << r[{}]", dest, lhs, rhs),
        },
        Insn::Variable { index, dest } => ExplainRow {
            addr,
            opcode: "Variable",
            p1: usize::from(*index) as i32,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=parameter({})", *dest, *index),
        },
        Insn::ZeroOrNull { rg1, rg2, dest } => ExplainRow {
            addr,
            opcode: "ZeroOrNull",
            p1: *rg1 as i32,
            p2: *dest as i32,
            p3: *rg2 as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "((r[{}]=NULL)|(r[{}]=NULL)) ? r[{}]=NULL : r[{}]=0",
                rg1, rg2, dest, dest
            ),
        },
        Insn::Not { reg, dest } => ExplainRow {
            addr,
            opcode: "Not",
            p1: *reg as i32,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=!r[{}]", dest, reg),
        },
        Insn::Concat { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "Concat",
            p1: *rhs as i32,
            p2: *lhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=r[{}] + r[{}]", dest, lhs, rhs),
        },
        Insn::And { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "And",
            p1: *rhs as i32,
            p2: *lhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=(r[{}] && r[{}])", dest, lhs, rhs),
        },
        Insn::Or { lhs, rhs, dest } => ExplainRow {
            addr,
            opcode: "Or",
            p1: *rhs as i32,
            p2: *lhs as i32,
            p3: *dest as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("r[{}]=(r[{}] || r[{}])", dest, lhs, rhs),
        },
        Insn::Noop => ExplainRow {
            addr,
            opcode: "Noop",
            p1: 0,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: String::new(),
        },
        Insn::PageCount { db, dest } => ExplainRow {
            addr,
            opcode: "Pagecount",
            p1: *db as i32,
            p2: *dest as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::ReadCookie { db, dest, cookie } => ExplainRow {
            addr,
            opcode: "ReadCookie",
            p1: *db as i32,
            p2: *dest as i32,
            p3: *cookie as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: "".to_string(),
        },
        Insn::AutoCommit {
            auto_commit,
            rollback,
        } => ExplainRow {
            addr,
            opcode: "AutoCommit",
            p1: *auto_commit as i32,
            p2: *rollback as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("auto_commit={}, rollback={}", auto_commit, rollback),
        },
        Insn::OpenEphemeral {
            cursor_id,
            is_table,
        } => ExplainRow {
            addr,
            opcode: "OpenEphemeral",
            p1: *cursor_id as i32,
            p2: *is_table as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "cursor={} is_table={}",
                cursor_id,
                if *is_table { "true" } else { "false" }
            ),
        },
        Insn::OpenAutoindex { cursor_id } => ExplainRow {
            addr,
            opcode: "OpenAutoindex",
            p1: *cursor_id as i32,
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("cursor={}", cursor_id),
        },
        Insn::Once {
            target_pc_when_reentered,
        } => ExplainRow {
            addr,
            opcode: "Once",
            p1: target_pc_when_reentered.to_debug_int(),
            p2: 0,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!("goto {}", target_pc_when_reentered.to_debug_int()),
        },
        Insn::BeginSubrtn { dest, dest_end } => ExplainRow {
            addr,
            opcode: "BeginSubrtn",
            p1: *dest as i32,
            p2: dest_end.map_or(0, |end| end as i32),
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: dest_end.map_or(format!("r[{}]=NULL", dest), |end| {
                format!("r[{}..{}]=NULL", dest, end)
            }),
        },
        Insn::NotFound {
            cursor_id,
            target_pc,
            record_reg,
            ..
        } => ExplainRow {
            addr,
            opcode: "NotFound",
            p1: *cursor_id as i32,
            p2: target_pc.to_debug_int(),
            p3: *record_reg as i32,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "if (r[{}] != NULL) goto {}",
                record_reg,
                target_pc.to_debug_int()
            ),
        },
        Insn::Affinity {
            start_reg,
            count,
            affinities,
        } => ExplainRow {
            addr,
            opcode: "Affinity",
            p1: *start_reg as i32,
            p2: count.get() as i32,
            p3: 0,
            p4: OwnedValue::build_text(""),
            p5: 0,
            comment: format!(
                "r[{}..{}] = {}",
                start_reg,
                start_reg + count.get(),
                affinities
                    .chars()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        },
    }
}

pub fn insn_to_str(
    program: &Program,
    addr: InsnReference,
    insn: &Insn,
    indent: String,
    manual_comment: Option<&'static str>,
) -> String {
    let explain_row = insn_to_explain_row(program, addr, insn);

    format!(
        "{:<4}  {:<17}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
        explain_row.addr,
        &(indent + explain_row.opcode),
        explain_row.p1,
        explain_row.p2,
        explain_row.p3,
        explain_row.p4.to_string(),
        explain_row.p5,
        manual_comment.map_or(explain_row.comment.clone(), |mc| format!(
            "{}; {}",
            explain_row.comment, mc
        ))
    )
}
