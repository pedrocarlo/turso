use crate::common::{ExecRows, TempDatabase};

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_fail_drop_indexed_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX i ON t (a)")?;
    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(res.is_err(), "Expected error when dropping indexed column");
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a UNIQUE, b);")]
fn test_fail_drop_unique_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(res.is_err(), "Expected error when dropping UNIQUE column");
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b, UNIQUE(a, b));")]
fn test_fail_drop_compound_unique_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column in compound UNIQUE"
    );
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a PRIMARY KEY, b);")]
fn test_fail_drop_primary_key_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping PRIMARY KEY column"
    );
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b, PRIMARY KEY(a, b));")]
fn test_fail_drop_compound_primary_key_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column in compound PRIMARY KEY"
    );
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_fail_drop_partial_index_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX i ON t (b) WHERE a > 0")?;
    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column referenced by partial index"
    );
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_fail_drop_view_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE VIEW v AS SELECT a, b FROM t")?;
    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column referenced by view"
    );
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_rename_view_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE VIEW v AS SELECT a, b FROM t")?;
    conn.execute("INSERT INTO t VALUES (1, 2)")?;
    conn.execute("ALTER TABLE t RENAME a TO c")?;
    let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT * FROM v");
    assert_eq!(rows, vec![(1, 2)]);
    let sql: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type = 'view' AND name = 'v'");
    assert_eq!(
        sql,
        vec![("CREATE VIEW v AS SELECT c, b FROM t".to_string(),)]
    );
    Ok(())
}

#[turso_macros::test(
    init_sql = "CREATE TABLE t (pk INTEGER PRIMARY KEY, indexed INTEGER, viewed INTEGER, partial INTEGER, compound1 INTEGER, compound2 INTEGER, unused1 INTEGER, unused2 INTEGER, unused3 INTEGER);"
)]
fn test_allow_drop_unreferenced_columns(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX idx ON t(indexed)")?;
    conn.execute("CREATE VIEW v AS SELECT viewed FROM t")?;
    conn.execute("CREATE INDEX partial_idx ON t(compound1) WHERE partial > 0")?;
    conn.execute("CREATE INDEX compound_idx ON t(compound1, compound2)")?;

    // Should be able to drop unused columns
    conn.execute("ALTER TABLE t DROP COLUMN unused1")?;
    conn.execute("ALTER TABLE t DROP COLUMN unused2")?;
    conn.execute("ALTER TABLE t DROP COLUMN unused3")?;

    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_supported(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(b INTEGER, a TEXT PRIMARY KEY, c TEXT) WITHOUT ROWID")?;

    let sql: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 't'");
    assert_eq!(
        sql,
        vec![("CREATE TABLE t (b INTEGER, a TEXT PRIMARY KEY, c TEXT) WITHOUT ROWID".to_string(),)]
    );
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_composite_pk_supported(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(a TEXT, b INT, PRIMARY KEY(a, b)) WITHOUT ROWID")?;
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_requires_primary_key(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("CREATE TABLE t(a TEXT, b INT) WITHOUT ROWID");
    assert!(
        res.is_err(),
        "Expected error when creating WITHOUT ROWID table without a primary key"
    );
    assert!(
        res.unwrap_err().to_string().contains("PRIMARY KEY"),
        "Expected error message about a required primary key"
    );
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_rejects_secondary_unique(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res =
        conn.execute("CREATE TABLE t(a TEXT PRIMARY KEY, b INT UNIQUE, c TEXT) WITHOUT ROWID");
    assert!(
        res.is_err(),
        "Expected error when creating WITHOUT ROWID table with secondary UNIQUE"
    );
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("secondary UNIQUE constraints on WITHOUT ROWID tables are not supported"),
        "Expected error message about secondary UNIQUE constraints"
    );
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_rejects_autoincrement(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT) WITHOUT ROWID");
    assert!(
        res.is_err(),
        "Expected error when creating WITHOUT ROWID table with AUTOINCREMENT"
    );
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("AUTOINCREMENT is not allowed on WITHOUT ROWID tables"),
        "Expected error message about AUTOINCREMENT"
    );
    Ok(())
}

#[turso_macros::test]
fn test_fail_not_null_in_upsert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER NOT NULL, c TEXT NOT NULL);")?;
    conn.execute("INSERT INTO t VALUES (1, 10, 'first');")?;

    let res = conn.execute("INSERT INTO t VALUES (1, NULL, 'second') ON CONFLICT(a) DO UPDATE SET b = excluded.b, c = excluded.c;");
    assert!(res.is_err(), "Expected NOT NULL constraint error");
    assert!(
        res.unwrap_err().to_string().contains("t.b"),
        "Expected NOT NULL error message to contain 't.b'"
    );
    Ok(())
}

/// test which simulation situation when prepared statement is used within a transaction which changed schema itself
/// in this case DB must not use database schema - but instead use connection schema
#[turso_macros::test]
fn test_prepared_stmt_reprepare_ddl_change_txn(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(x);")?;
    let mut stmt = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE q(x)").unwrap();
    stmt.run_ignore_rows().unwrap();
    conn.execute("COMMIT").unwrap();

    Ok(())
}

/// Older Turso versions stored CREATE VIEW column lists without identifier
/// quoting, leaving sqlite_schema rows whose SQL no longer parses (and which
/// real SQLite refuses to load entirely). Such rows are tolerated at schema
/// load and must be removable with DROP VIEW so affected databases can be
/// cleaned up and the name reused.
///
/// The fixture was generated by a pre-fix tursodb running
/// `CREATE VIEW v([col one]) AS SELECT a FROM t`. Read-only assertions on
/// the same fixture live in
/// testing/sqltests/turso-tests/legacy-unquoted-view-columns.sqltest.
#[test]
fn test_drop_broken_legacy_view_row() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../testing/sqltests/database/testing_legacy_unquoted_view_columns.db");
    let tmp_dir = tempfile::TempDir::new()?;
    let db_path = tmp_dir.path().join("legacy_view.db");
    std::fs::copy(&fixture, &db_path)?;

    let db = TempDatabase::builder().with_db_path(&db_path).build();
    let conn = db.connect_limbo();

    // The table is readable; the broken view is unavailable with a
    // diagnosable error; CREATE VIEW over the name is blocked.
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT a FROM t");
    assert_eq!(rows, vec![(42,)]);
    let err = conn.execute("SELECT * FROM v").unwrap_err();
    assert!(err.to_string().contains("could not be loaded"), "{err}");
    let err = conn
        .execute("CREATE VIEW v AS SELECT a FROM t")
        .unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");

    // DROP VIEW removes the orphaned row and frees the name.
    conn.execute("DROP VIEW v")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM sqlite_master WHERE name = 'v'");
    assert_eq!(rows, vec![(0,)]);
    conn.execute("CREATE VIEW v(\"col one\") AS SELECT a FROM t")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT \"col one\" FROM v");
    assert_eq!(rows, vec![(42,)]);
    Ok(())
}

/// Reproducer for the Antithesis finding in ENG-74 (and the bug family in
/// https://github.com/tursodatabase/turso/issues/7728): a statement paused at
/// its root-page read used to keep using that root page after DROP TABLE freed
/// it and a subsequent CREATE TABLE + INSERT recycled it with different
/// content. The resumed seek then descended through stale/recycled pages and
/// tripped `matches!(self.page_type(), Ok(PageType::TableInterior))` in
/// `PageInner::cell_table_interior_read_rowid`.
///
/// Like SQLite's OP_Destroy, DROP TABLE must instead fail with "database
/// table is locked" while another statement is active on the connection, and
/// succeed once that statement finishes.
#[test]
fn active_table_seek_after_drop_reuse_must_not_use_recycled_root_page() -> anyhow::Result<()> {
    use crate::queued_io::QueuedIo;
    use std::sync::Arc;
    use turso_core::{Database, DatabaseOpts, OpenFlags, StepResult};

    let io = Arc::new(QueuedIo::new());
    let tmp_dir = tempfile::TempDir::new()?;
    let path = tmp_dir.path().join("table-interior-drop-reuse.db");
    let path = path.to_str().unwrap();
    let db =
        Database::open_file_with_flags(io, path, OpenFlags::default(), DatabaseOpts::new(), None)?;
    let conn = db.connect()?;

    conn.execute("PRAGMA page_size=512")?;
    conn.execute("PRAGMA cache_size=9")?;
    conn.execute("PRAGMA cache_spill=ON")?;
    conn.execute("PRAGMA journal_mode='wal'")?;
    conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, b BLOB)")?;
    for id in 1..=32 {
        conn.execute(format!("INSERT INTO u VALUES({id}, zeroblob(60))").as_str())?;
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

    // Pause the SELECT at its first I/O: the read of the (interior) root page
    // of `u`.
    let mut select = conn.prepare("SELECT id, length(b) FROM u WHERE id = 16")?;
    match select.step()? {
        StepResult::IO => select._io().step()?,
        other => anyhow::bail!("SELECT did not yield at the root-page read: {other:?}"),
    }

    // Freeing `u`'s root page while the SELECT is still active must be
    // refused, otherwise the recycled page would be misread as `u`'s B-tree
    // when the SELECT resumes.
    let err = conn
        .execute("DROP TABLE u")
        .expect_err("DROP TABLE must fail while another statement is active");
    assert!(
        err.to_string().contains("database table is locked"),
        "expected table-locked error, got: {err}"
    );

    // The paused SELECT resumes against an intact tree and finds its row.
    let mut rows = Vec::new();
    loop {
        match select.step()? {
            StepResult::IO => select._io().step()?,
            StepResult::Yield => {}
            StepResult::Row => {
                let row = select.row().expect("row should be available after Row");
                rows.push((row.get::<i64>(0)?, row.get::<i64>(1)?));
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => {
                anyhow::bail!("unexpected non-progress result while draining statement")
            }
        }
    }
    assert_eq!(rows, vec![(16, 60)]);

    // Once the reader has finished, DROP TABLE succeeds and the freed pages
    // can be recycled safely.
    conn.execute("DROP TABLE u")?;
    conn.execute("CREATE TABLE reuse(id INTEGER PRIMARY KEY, b BLOB)")?;
    conn.execute("INSERT INTO reuse VALUES(16, zeroblob(5000))")?;
    let mut check = conn.prepare("PRAGMA integrity_check")?;
    loop {
        match check.step()? {
            StepResult::IO => check._io().step()?,
            StepResult::Yield => {}
            StepResult::Row => {
                let row = check.row().expect("row should be available after Row");
                assert_eq!(row.get::<String>(0)?, "ok");
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => {
                anyhow::bail!("unexpected non-progress result while draining statement")
            }
        }
    }

    Ok(())
}

/// SQLite parity check for the scenario above, driven through rusqlite:
/// keeping two statements in flight on one connection is legal API usage in
/// SQLite (not undefined behavior), and SQLite answers the DROP TABLE with
/// SQLITE_LOCKED ("database table is locked") while the reader is active,
/// then allows it once the reader finishes. Our `op_destroy` guard mirrors
/// exactly this behavior.
#[test]
fn sqlite_rejects_drop_table_while_statement_active_parity() -> anyhow::Result<()> {
    let tmp_dir = tempfile::TempDir::new()?;
    let path = tmp_dir.path().join("sqlite-drop-while-active.db");
    let conn = rusqlite::Connection::open(&path)?;
    conn.pragma_update(None, "page_size", 512)?;
    conn.pragma_update(None, "cache_size", 9)?;
    conn.query_row("PRAGMA journal_mode='wal'", [], |_| Ok(()))?;
    conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, b BLOB)", [])?;
    for id in 1..=32 {
        conn.execute("INSERT INTO u VALUES(?1, zeroblob(60))", [id])?;
    }

    // Fetch the row but do not step the statement to SQLITE_DONE: the
    // statement stays active mid-execution, like the paused Turso SELECT.
    let mut select = conn.prepare("SELECT id, length(b) FROM u WHERE id = 16")?;
    let mut rows = select.query([])?;
    let row = rows.next()?.expect("row for id 16 should exist");
    assert_eq!(row.get::<_, i64>(0)?, 16);
    assert_eq!(row.get::<_, i64>(1)?, 60);

    let err = conn
        .execute("DROP TABLE u", [])
        .expect_err("DROP TABLE must fail while another statement is active");
    assert!(
        err.to_string().contains("database table is locked"),
        "expected table-locked error, got: {err}"
    );

    // Drain the reader; DROP TABLE then succeeds and the pages can be
    // recycled safely.
    assert!(rows.next()?.is_none());
    conn.execute("DROP TABLE u", [])?;
    conn.execute("CREATE TABLE reuse(id INTEGER PRIMARY KEY, b BLOB)", [])?;
    conn.execute("INSERT INTO reuse VALUES(16, zeroblob(5000))", [])?;
    let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    assert_eq!(ok, "ok");
    Ok(())
}
