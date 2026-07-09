use crate::schema::Schema;
use crate::translate::emitter::{emit_cdc_explicit_commit_insns, Resolver, TransactionMode};
use crate::translate::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::emission::{Either, Repeat};
use crate::vdbe::insn::Insn;
use crate::Result;
use turso_parser::ast::{Name, TransactionType};

pub fn translate_tx_begin(
    tx_type: Option<TransactionType>,
    _tx_name: Option<Name>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let tx_type = tx_type.unwrap_or(TransactionType::Deferred);
    let autocommit = Insn::AutoCommit {
        auto_commit: false,
        rollback: false,
    };
    let emission = match tx_type {
        // SQLite emits only AutoCommit for deferred — no
        // Transaction opcodes at all (for any database).
        TransactionType::Deferred => Either::Left(autocommit),
        // SQLite emits Transaction for every open database (main, temp, each attached)
        // on BEGIN IMMEDIATE / EXCLUSIVE / CONCURRENT. We match that exactly. For temp,
        // this may trigger lazy initialization via `ensure_temp_database` in
        // op_transaction: an acceptable one-time cost that keeps the opcode sequence
        // identical to SQLite. Concurrent differs from Immediate/Exclusive only in the
        // main database's tx mode: temp has no MVCC, so it uses a plain write lock even
        // in Concurrent mode (op_transaction detects this via
        // `mv_store_for_db(TEMP) == None` and skips the MVCC path).
        TransactionType::Immediate | TransactionType::Exclusive | TransactionType::Concurrent => {
            let main_tx_mode = if matches!(tx_type, TransactionType::Concurrent) {
                TransactionMode::Concurrent
            } else {
                TransactionMode::Write
            };
            Either::Right((
                Insn::Transaction {
                    db: crate::MAIN_DB_ID,
                    tx_mode: main_tx_mode,
                    schema_cookie: resolver.schema().schema_version,
                },
                Insn::Transaction {
                    db: crate::TEMP_DB_ID,
                    tx_mode: TransactionMode::Write,
                    schema_cookie: resolver.with_schema(crate::TEMP_DB_ID, |s| s.schema_version),
                },
                Repeat::new(resolver.attached_database_ids_in_search_order()?, |db_id| {
                    Insn::Transaction {
                        db: db_id,
                        tx_mode: TransactionMode::Write,
                        schema_cookie: resolver.with_schema(db_id, |s| s.schema_version),
                    }
                }),
                autocommit,
            ))
        }
    };
    program.emit_all(emission);
    Ok(())
}

pub fn translate_tx_commit(
    _tx_name: Option<Name>,
    schema: &Schema,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    program.extend(&ProgramBuilderOpts::new(0, 0, 0));

    let cdc_info = program.capture_data_changes_info().as_ref();
    if cdc_info.is_some_and(|info| info.cdc_version().has_commit_record()) {
        emit_cdc_explicit_commit_insns(program, schema, resolver)?;
    }

    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: false,
    });
    Ok(())
}
