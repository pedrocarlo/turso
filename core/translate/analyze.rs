use crate::sync::Arc;

use crate::{
    bail_parse_error,
    function::{Func, FuncCtx, ScalarFunc},
    schema::{BTreeTable, Index, RESERVED_TABLE_PREFIXES},
    storage::pager::CreateBTreeFlags,
    translate::{
        collate::CollationSeq,
        emit_monad::{
            alloc_label, alloc_labels, alloc_reg, alloc_regs, column, copy, function_call, goto,
            insert, integer, is_null, make_record, ne_jump, new_rowid, next, preassign_label,
            rewind, static_iter, string8, when, Emit, LoopEmit,
        },
        emitter::Resolver,
        schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID},
    },
    util::normalize_ident,
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder},
        insn::{to_u16, CmpInsFlags, Cookie, Insn, RegisterOrLiteral},
        BranchOffset,
    },
    Result,
};
use turso_parser::ast;

pub fn translate_analyze(
    target_opt: Option<ast::QualifiedName>,
    resolver: &Resolver,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    // Collect all analyze targets up front so we can create/open sqlite_stat1 just once.
    let analyze_targets: Vec<(Arc<BTreeTable>, Option<Arc<Index>>)> = match target_opt {
        Some(target) => {
            let normalized = normalize_ident(target.name.as_str());
            let db_normalized = target
                .db_name
                .as_ref()
                .map(|db| normalize_ident(db.as_str()));
            let target_is_main =
                normalized.eq_ignore_ascii_case("main") || db_normalized.as_deref() == Some("main");
            if target_is_main {
                resolver
                    .schema
                    .tables
                    .iter()
                    .filter_map(|(name, table)| {
                        if RESERVED_TABLE_PREFIXES
                            .iter()
                            .any(|prefix| name.starts_with(prefix))
                        {
                            return None;
                        }
                        table.btree().map(|bt| (bt, None))
                    })
                    .collect()
            } else if let Some(table) = resolver.schema.get_btree_table(&normalized) {
                vec![(
                    table.clone(),
                    None, // analyze the whole table and its indexes
                )]
            } else {
                // Try to find an index by this name.
                let mut found: Option<(Arc<BTreeTable>, Arc<Index>)> = None;
                for (table_name, indexes) in resolver.schema.indexes.iter() {
                    if let Some(index) = indexes
                        .iter()
                        .find(|idx| idx.name.eq_ignore_ascii_case(&normalized))
                    {
                        if let Some(table) = resolver.schema.get_btree_table(table_name) {
                            found = Some((table, index.clone()));
                            break;
                        }
                    }
                }
                let Some((table, index)) = found else {
                    bail_parse_error!("no such table or index: {}", target.name);
                };
                vec![(table.clone(), Some(index))]
            }
        }
        None => resolver
            .schema
            .tables
            .iter()
            .filter_map(|(name, table)| {
                if RESERVED_TABLE_PREFIXES
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
                {
                    return None;
                }
                table.btree().map(|bt| (bt, None))
            })
            .collect(),
    };

    if analyze_targets.is_empty() {
        return Ok(program);
    }

    // This is emitted early because SQLite does, and thus generated VDBE matches a bit closer.
    let null_reg = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: null_reg,
        dest_end: None,
    });

    // After preparing/creating sqlite_stat1, we need to OpenWrite it, and how we acquire
    // the necessary BTreeTable for cursor creation and root page for the instruction changes
    // depending on which path we take.
    let sqlite_stat1_btreetable: Arc<BTreeTable>;
    let sqlite_stat1_source: RegisterOrLiteral<_>;

    if let Some(sqlite_stat1) = resolver.schema.get_btree_table("sqlite_stat1") {
        sqlite_stat1_btreetable = sqlite_stat1.clone();
        sqlite_stat1_source = RegisterOrLiteral::Literal(sqlite_stat1.root_page);
    } else {
        // FIXME: Emit ReadCookie 0 3 2
        // FIXME: Emit If 3 +2 0
        // FIXME: Emit SetCookie 0 2 4
        // FIXME: Emit SetCookie 0 5 1

        // See the large comment in schema.rs:translate_create_table about
        // deviating from SQLite codegen, as the same deviation is being done
        // here.

        // TODO: this code half-copies translate_create_table, because there's
        // no way to get the table_root_reg back out, and it's needed for later
        // codegen to open the table we just created.  It's worth a future
        // refactoring to remove the duplication one the rest of ANALYZE is
        // implemented.
        let table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: 0,
            root: table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        let sql = "CREATE TABLE sqlite_stat1(tbl,idx,stat)";
        // The root_page==0 is false, but we don't rely on it, and there's no
        // way to initialize it with a correct value.
        sqlite_stat1_btreetable = Arc::new(BTreeTable::from_sql(sql, 0)?);
        sqlite_stat1_source = RegisterOrLiteral::Register(table_root_reg);

        let table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
        let sqlite_schema_cursor_id =
            program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: sqlite_schema_cursor_id,
            root_page: 1i64.into(),
            db: 0,
        });

        // Add the table entry to sqlite_schema
        emit_schema_entry(
            &mut program,
            resolver,
            sqlite_schema_cursor_id,
            None,
            SchemaEntryType::Table,
            "sqlite_stat1",
            "sqlite_stat1",
            table_root_reg,
            Some(sql.to_string()),
        )?;

        let parse_schema_where_clause =
            "tbl_name = 'sqlite_stat1' AND type != 'trigger'".to_string();
        program.emit_insn(Insn::ParseSchema {
            db: sqlite_schema_cursor_id,
            where_clause: Some(parse_schema_where_clause),
        });

        // Bump schema cookie so subsequent statements reparse schema.
        program.emit_insn(Insn::SetCookie {
            db: 0,
            cookie: Cookie::SchemaVersion,
            value: resolver.schema.schema_version as i32 + 1,
            p5: 0,
        });
    };

    // Count the number of rows in the target table(s), and insert into sqlite_stat1.
    let sqlite_stat1 = sqlite_stat1_btreetable;
    let stat_cursor = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_stat1.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: stat_cursor,
        root_page: sqlite_stat1_source,
        db: 0,
    });

    for (target_table, target_index) in analyze_targets {
        if !target_table.has_rowid {
            bail_parse_error!("ANALYZE on tables without rowid is not supported");
        }

        // Remove existing stat rows for this target before inserting fresh ones.
        let rewind_done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: stat_cursor,
            pc_if_empty: rewind_done,
        });
        let loop_start = program.allocate_label();
        program.preassign_label_to_next_insn(loop_start);

        let tbl_col_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: stat_cursor,
            column: 0,
            dest: tbl_col_reg,
            default: None,
        });
        let target_tbl_reg = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: target_table.name.to_string(),
            dest: target_tbl_reg,
        });
        program.mark_last_insn_constant();

        let skip_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: tbl_col_reg,
            rhs: target_tbl_reg,
            target_pc: skip_label,
            flags: Default::default(),
            collation: None,
        });

        if let Some(idx) = target_index.clone() {
            let idx_col_reg = program.alloc_register();
            program.emit_insn(Insn::Column {
                cursor_id: stat_cursor,
                column: 1,
                dest: idx_col_reg,
                default: None,
            });
            let target_idx_reg = program.alloc_register();
            program.emit_insn(Insn::String8 {
                value: idx.name.to_string(),
                dest: target_idx_reg,
            });
            program.mark_last_insn_constant();
            program.emit_insn(Insn::Ne {
                lhs: idx_col_reg,
                rhs: target_idx_reg,
                target_pc: skip_label,
                flags: Default::default(),
                collation: None,
            });
            let rowid_reg = program.alloc_register();
            program.emit_insn(Insn::RowId {
                cursor_id: stat_cursor,
                dest: rowid_reg,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: stat_cursor,
                table_name: "sqlite_stat1".to_string(),
                is_part_of_update: false,
            });
            program.emit_insn(Insn::Next {
                cursor_id: stat_cursor,
                pc_if_next: loop_start,
            });
        } else {
            let rowid_reg = program.alloc_register();
            program.emit_insn(Insn::RowId {
                cursor_id: stat_cursor,
                dest: rowid_reg,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: stat_cursor,
                table_name: "sqlite_stat1".to_string(),
                is_part_of_update: false,
            });
            program.emit_insn(Insn::Next {
                cursor_id: stat_cursor,
                pc_if_next: loop_start,
            });
        }

        program.preassign_label_to_next_insn(skip_label);
        program.emit_insn(Insn::Next {
            cursor_id: stat_cursor,
            pc_if_next: loop_start,
        });
        program.preassign_label_to_next_insn(rewind_done);

        let target_cursor = program.alloc_cursor_id(CursorType::BTreeTable(target_table.clone()));
        program.emit_insn(Insn::OpenRead {
            cursor_id: target_cursor,
            root_page: target_table.root_page,
            db: 0,
        });
        let rowid_reg = program.alloc_register();
        let tablename_reg = program.alloc_register();
        let indexname_reg = program.alloc_register();
        let stat_text_reg = program.alloc_register();
        let record_reg = program.alloc_register();
        let count_reg = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: target_table.name.to_string(),
            dest: tablename_reg,
        });
        program.mark_last_insn_constant();
        program.emit_insn(Insn::Count {
            cursor_id: target_cursor,
            target_reg: count_reg,
            exact: true,
        });
        let after_insert = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: count_reg,
            target_pc: after_insert,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Null {
            dest: indexname_reg,
            dest_end: None,
        });
        // stat = CAST(count AS TEXT)
        program.emit_insn(Insn::Copy {
            src_reg: count_reg,
            dst_reg: stat_text_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Cast {
            reg: stat_text_reg,
            affinity: Affinity::Text,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(tablename_reg),
            count: to_u16(3),
            dest_reg: to_u16(record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: stat_cursor,
            rowid_reg,
            prev_largest_reg: 0,
        });
        // FIXME: SQLite sets OPFLAG_APPEND on the insert, but that's not supported in turso right now.
        // SQLite doesn't emit the table name, but like... why not?
        program.emit_insn(Insn::Insert {
            cursor: stat_cursor,
            key_reg: rowid_reg,
            record_reg,
            flag: Default::default(),
            table_name: "sqlite_stat1".to_string(),
        });
        program.preassign_label_to_next_insn(after_insert);
        // Emit index stats for this table (or for a single index target).
        let indexes: Vec<Arc<Index>> = match target_index {
            Some(idx) => vec![idx],
            None => resolver
                .schema
                .get_indices(&target_table.name)
                .filter(|idx| idx.index_method.is_none()) // skip custom for now
                .cloned()
                .collect(),
        };
        for index in indexes {
            emit_index_stats(&mut program, stat_cursor, &target_table, &index);
        }
    }

    // FIXME: Emit LoadAnalysis
    // FIXME: Emit Expire
    Ok(program)
}

/// Emit VDBE code to gather and insert statistics for a single index.
///
/// This uses the stat_init/stat_push/stat_get functions to collect statistics.
/// The bytecode scans the index in sorted order, comparing columns to detect
/// when prefixes change, and calls stat_push with the change index.
///
/// The stat string format is: "total avg1 avg2 avg3"
/// where avgN = ceil(total / distinctN) = average rows per distinct prefix
fn emit_index_stats(
    program: &mut ProgramBuilder,
    stat_cursor: usize,
    table: &Arc<BTreeTable>,
    index: &Arc<Index>,
) {
    let n_cols = index.columns.len();
    if n_cols == 0 {
        return;
    }

    // Open the index cursor (imperative - needs cursor allocation)
    let idx_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: idx_cursor,
        root_page: index.root_page,
        db: 0,
    });

    // Build the monadic computation for the rest
    let table_name = table.name.clone();
    let index_name = index.name.clone();
    let column_collations: Vec<_> = index.columns.iter().map(|c| c.collation).collect();

    let computation = emit_index_stats_monadic(
        idx_cursor,
        stat_cursor,
        n_cols,
        table_name,
        index_name,
        column_collations,
    );

    // Run the monadic computation
    computation.run(program).expect("emit_index_stats failed");
}

/// Monadic implementation of index statistics emission.
///
/// This demonstrates the declarative, composable style where the bytecode
/// structure is described rather than imperatively emitted.
///
/// Uses tuple-based allocation to avoid deep nesting.
fn emit_index_stats_monadic(
    idx_cursor: usize,
    stat_cursor: usize,
    n_cols: usize,
    table_name: String,
    index_name: String,
    column_collations: Vec<Option<CollationSeq>>,
) -> impl Emit<Output = ()> {
    // Allocate all registers and labels in one flat tuple
    (
        alloc_reg(),          // reg_accum
        alloc_reg(),          // reg_chng
        alloc_regs(n_cols),   // reg_prev_base
        alloc_reg(),          // reg_temp
        alloc_label(),        // lbl_empty
        alloc_label(),        // lbl_loop
        alloc_label(),        // lbl_stat_push
        alloc_labels(n_cols), // lbl_update_prev
    )
        .then(
            move |(
                reg_accum,
                reg_chng,
                reg_prev_base,
                reg_temp,
                lbl_empty,
                lbl_loop,
                lbl_stat_push,
                lbl_update_prev,
            )| {
                // Clone for inner closures that need ownership
                let lbl_update_prev_clone = lbl_update_prev.clone();

                // Initialize accumulator with stat_init(n_cols)
                integer(n_cols as i64, reg_chng)
                    .and_then(function_call(
                        reg_chng,
                        reg_accum,
                        FuncCtx {
                            func: Func::Scalar(ScalarFunc::StatInit),
                            arg_count: 1,
                        },
                        0,
                    ))
                    // Rewind cursor; if empty, jump to end
                    .and_then(rewind(idx_cursor, lbl_empty))
                    // First row: chng=0, jump to update all prev columns
                    .and_then(integer(0, reg_chng))
                    .and_then(goto(lbl_update_prev[0]))
                    // Main loop label
                    .and_then(preassign_label(lbl_loop))
                    // Reset chng = 0
                    .and_then(integer(0, reg_chng))
                    // Compare each column
                    .and_then(emit_column_comparisons(
                        idx_cursor,
                        n_cols,
                        reg_temp,
                        reg_prev_base,
                        reg_chng,
                        lbl_update_prev_clone.clone(),
                        column_collations,
                    ))
                    // All columns equal - duplicate row
                    .and_then(integer(n_cols as i64, reg_chng))
                    .and_then(goto(lbl_stat_push))
                    // Update prev section
                    .and_then(emit_update_prev_section(
                        idx_cursor,
                        n_cols,
                        reg_prev_base,
                        lbl_update_prev_clone,
                    ))
                    // stat_push
                    .and_then(preassign_label(lbl_stat_push))
                    .and_then(function_call(
                        reg_accum,
                        reg_accum,
                        FuncCtx {
                            func: Func::Scalar(ScalarFunc::StatPush),
                            arg_count: 2,
                        },
                        0,
                    ))
                    // Next iteration
                    .and_then(next(idx_cursor, lbl_loop))
                    // Get final stat string
                    .then(move |_| {
                        emit_stat_insert(stat_cursor, reg_accum, lbl_empty, table_name, index_name)
                    })
                    // Empty label at end
                    .and_then(preassign_label(lbl_empty))
            },
        )
}

/// Emit column comparisons for the main loop.
///
/// Uses `static_iter` from LoopEmit to iterate over columns at compile time.
fn emit_column_comparisons(
    idx_cursor: usize,
    n_cols: usize,
    reg_temp: usize,
    reg_prev_base: usize,
    reg_chng: usize,
    lbl_update_prev: Vec<BranchOffset>,
    column_collations: Vec<Option<CollationSeq>>,
) -> impl Emit<Output = ()> {
    // Use static_iter to emit comparison code for each column
    static_iter(0..n_cols, move |i| {
        let lbl = lbl_update_prev[i];
        let collation = column_collations[i];
        let is_last = i == n_cols - 1;

        // Read column into temp, compare with prev
        column(idx_cursor, i, reg_temp)
            .and_then(ne_jump(
                reg_temp,
                reg_prev_base + i,
                lbl,
                CmpInsFlags::default().null_eq(),
                collation,
            ))
            // If not last and columns match, set chng to i+1
            .and_then(when(!is_last, move || integer((i + 1) as i64, reg_chng)))
    })
    .emit_all()
}

/// Emit the update_prev section where we update previous column values.
///
/// Uses `static_iter` from LoopEmit to emit code for each column.
fn emit_update_prev_section(
    idx_cursor: usize,
    n_cols: usize,
    reg_prev_base: usize,
    lbl_update_prev: Vec<BranchOffset>,
) -> impl Emit<Output = ()> {
    // Use static_iter to emit update code for each column
    static_iter(0..n_cols, move |i| {
        let lbl = lbl_update_prev[i];
        preassign_label(lbl).and_then(column(idx_cursor, i, reg_prev_base + i))
    })
    .emit_all()
}

/// Emit stat_get and insert into sqlite_stat1.
fn emit_stat_insert(
    stat_cursor: usize,
    reg_accum: usize,
    lbl_empty: BranchOffset,
    table_name: String,
    index_name: String,
) -> impl Emit<Output = ()> {
    // Allocate register for stat result
    alloc_reg().then(move |reg_stat| {
        // Call stat_get
        function_call(
            reg_accum,
            reg_stat,
            FuncCtx {
                func: Func::Scalar(ScalarFunc::StatGet),
                arg_count: 1,
            },
            0,
        )
        // Skip insert if NULL
        .and_then(is_null(reg_stat, lbl_empty))
        // Allocate record registers: tablename, indexname, stat
        .then(move |_| {
            alloc_regs(3).then(move |record_start| {
                string8(table_name.clone(), record_start)
                    .and_then(string8(index_name.clone(), record_start + 1))
                    .and_then(copy(reg_stat, record_start + 2))
                    // Make record and insert
                    .then(move |_| {
                        alloc_reg().then(move |idx_record_reg| {
                            make_record(record_start, 3, idx_record_reg).then(move |_| {
                                alloc_reg().then(move |idx_rowid_reg| {
                                    new_rowid(stat_cursor, idx_rowid_reg).and_then(insert(
                                        stat_cursor,
                                        idx_rowid_reg,
                                        idx_record_reg,
                                        "sqlite_stat1".to_string(),
                                    ))
                                })
                            })
                        })
                    })
            })
        })
    })
}
