---
name: testing-enhancements
description: Recommended testing methodologies to enhance Turso's quality assurance
---
# Testing Enhancement Recommendations

This document outlines additional testing methodologies that could strengthen Turso's testing infrastructure. These recommendations are categorized by implementation complexity and expected value.

---

## High-Priority Recommendations

### 1. Mutation Testing with `cargo-mutants`

**What it is:** Mutation testing introduces small changes (mutations) to source code and verifies that tests detect them. If a test suite doesn't catch a mutation, it indicates a gap in test coverage.

**Why it matters for Turso:** A database must catch subtle bugs. Mutation testing measures test *quality*, not just coverage. A query optimizer bug that returns wrong results is catastrophic.

**Tool:** [cargo-mutants](https://github.com/sourcefrog/cargo-mutants)

**Implementation:**
```bash
cargo install cargo-mutants
cargo mutants --package turso_core --timeout 60
```

**Integration approach:**
```yaml
# .github/workflows/mutation.yml
name: Mutation Testing
on:
  schedule:
    - cron: '0 3 * * 0'  # Weekly on Sunday
jobs:
  mutants:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo install cargo-mutants
      - run: cargo mutants --package turso_core -j 4 --timeout 120
```

**Expected value:** Identifies weak tests that pass despite bugs. Particularly valuable for parser, optimizer, and transaction code.

---

### 2. Deeper SQLancer Integration

**What it is:** [SQLancer](https://github.com/sqlancer/sqlancer) is a tool specifically designed to find logic bugs in database systems. It implements multiple oracle techniques:

- **TLP (Ternary Logic Partitioning):** Partitions query results and checks consistency
- **PQS (Pivoted Query Synthesis):** Generates queries with known results
- **NoREC (Non-optimizing Reference Engine Comparison):** Compares optimized vs unoptimized execution

**Current state:** Turso has `build-sqlancer.yml` but it's on-demand only.

**Recommended enhancement:**
1. Run SQLancer nightly against Turso
2. Build a custom Turso adapter for SQLancer (Java)
3. Focus on TLP for finding optimizer bugs

**Implementation:**
```java
// sqlancer/src/sqlancer/turso/TursoProvider.java
public class TursoProvider extends SQLProviderAdapter<TursoGlobalState, TursoOptions> {
    // Implement Turso-specific connection and query execution
}
```

**Expected value:** SQLancer has found hundreds of bugs in SQLite, MySQL, PostgreSQL, and other databases. It's the gold standard for database logic bug detection.

---

### 3. Miri for Unsafe Code Verification

**What it is:** [Miri](https://github.com/rust-lang/miri) is an interpreter for Rust's mid-level intermediate representation (MIR) that can detect undefined behavior in unsafe code.

**Why it matters:** Turso likely has unsafe code in performance-critical paths (B-tree operations, memory mapping, I/O). Miri catches:
- Use after free
- Invalid pointer dereferences
- Data races (with `-Zmiri-check-concurrency`)
- Uninitialized memory reads

**Implementation:**
```bash
rustup +nightly component add miri
cargo +nightly miri test --package turso_core -- --test-threads=1
```

**CI Integration:**
```yaml
# Add to rust.yml
miri:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - run: rustup toolchain install nightly --component miri
    - run: cargo +nightly miri test -p turso_core --lib -- --test-threads=1
      env:
        MIRIFLAGS: '-Zmiri-disable-isolation'
```

**Expected value:** Catches memory safety bugs that Rust's borrow checker can't see in unsafe blocks. Critical for a database handling user data.

---

### 4. Crash Recovery Testing with Fault Injection

**What it is:** Systematic testing of database recovery after crashes at various points in transaction processing.

**Current state:** Antithesis provides some crash testing, but more deterministic injection would help.

**Recommended approach - Build custom crash injector:**

```rust
// testing/crash-injector/src/lib.rs
use std::sync::atomic::{AtomicUsize, Ordering};

static CRASH_COUNTDOWN: AtomicUsize = AtomicUsize::new(usize::MAX);

pub fn set_crash_after_n_writes(n: usize) {
    CRASH_COUNTDOWN.store(n, Ordering::SeqCst);
}

pub fn maybe_crash_on_write() {
    let remaining = CRASH_COUNTDOWN.fetch_sub(1, Ordering::SeqCst);
    if remaining == 1 {
        std::process::abort();  // Simulate crash
    }
}

// Integration test
#[test]
fn test_recovery_after_crash_during_commit() {
    let db_path = temp_db_path();

    // Phase 1: Write data, crash during commit
    set_crash_after_n_writes(5);
    let result = std::panic::catch_unwind(|| {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE t(x)").unwrap();
        conn.execute("BEGIN").unwrap();
        for i in 0..100 {
            conn.execute(&format!("INSERT INTO t VALUES({})", i)).unwrap();
        }
        conn.execute("COMMIT").unwrap();  // Should crash here
    });

    // Phase 2: Reopen and verify consistency
    let conn = Connection::open(&db_path).unwrap();
    let result = conn.query("PRAGMA integrity_check").unwrap();
    assert_eq!(result, "ok");
}
```

**Alternative - Use existing tools:**
- [CrashMonkey](https://github.com/utsaslab/crashmonkey) - File system crash consistency testing
- [dm-log-writes](https://docs.kernel.org/admin-guide/device-mapper/log-writes.html) - Linux device mapper for crash simulation

**Expected value:** Ensures WAL recovery works correctly under all crash scenarios. Prevents data loss bugs.

---

### 5. Property-Based Testing with `proptest`

**What it is:** Property-based testing generates random inputs and verifies that invariants hold. Unlike example-based tests, it explores the input space more thoroughly.

**Current state:** Some fuzz tests exist, but proptest provides better shrinking and reproducibility.

**Recommended properties to test:**

```rust
// tests/property/query_properties.rs
use proptest::prelude::*;

proptest! {
    #[test]
    fn query_order_independence(
        values in prop::collection::vec(any::<i64>(), 0..1000)
    ) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();

        for v in &values {
            conn.execute(&format!("INSERT INTO t VALUES({})", v)).unwrap();
        }

        // Property: COUNT should equal insertion count
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
        prop_assert_eq!(count, values.len() as i64);

        // Property: SUM should equal sum of inputs
        let sum: i64 = conn.query_row("SELECT COALESCE(SUM(x), 0) FROM t").unwrap();
        prop_assert_eq!(sum, values.iter().sum::<i64>());
    }

    #[test]
    fn transaction_atomicity(
        ops in prop::collection::vec(
            prop_oneof![
                Just(Op::Insert(rand_value())),
                Just(Op::Delete),
                Just(Op::Commit),
                Just(Op::Rollback),
            ],
            1..100
        )
    ) {
        // Verify transaction boundaries are respected
        // Either all ops in a transaction are visible, or none
    }

    #[test]
    fn index_consistency(
        values in prop::collection::vec(any::<String>(), 0..500)
    ) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE t(x TEXT)").unwrap();
        conn.execute("CREATE INDEX idx ON t(x)").unwrap();

        for v in &values {
            conn.execute_params("INSERT INTO t VALUES(?)", [v]).unwrap();
        }

        // Property: Index scan should return same results as table scan
        let idx_results: Vec<String> = conn.query(
            "SELECT x FROM t INDEXED BY idx ORDER BY x"
        ).unwrap();
        let tbl_results: Vec<String> = conn.query(
            "SELECT x FROM t NOT INDEXED ORDER BY x"
        ).unwrap();

        prop_assert_eq!(idx_results, tbl_results);
    }
}
```

**Expected value:** Finds edge cases that manual test cases miss. The shrinking feature produces minimal failing examples.

---

## Medium-Priority Recommendations

### 6. Metamorphic Testing

**What it is:** Testing that certain transformations preserve query results. If `Q1` and `Q2` are semantically equivalent, they must return the same results.

**Implementation:**
```rust
// testing/metamorphic/src/lib.rs

/// Metamorphic relations for SQL queries
enum MetamorphicRelation {
    /// SELECT * FROM t WHERE c1 AND c2 ≡ SELECT * FROM t WHERE c2 AND c1
    CommutativeAnd,
    /// SELECT * FROM t WHERE NOT NOT c ≡ SELECT * FROM t WHERE c
    DoubleNegation,
    /// SELECT * FROM (SELECT * FROM t) ≡ SELECT * FROM t
    SubqueryElimination,
    /// SELECT a FROM t UNION SELECT a FROM t ≡ SELECT DISTINCT a FROM t
    UnionIdempotent,
    /// COUNT(*) WHERE false = 0
    EmptyResultCount,
}

fn test_metamorphic_relation(conn: &Connection, relation: MetamorphicRelation) {
    match relation {
        MetamorphicRelation::CommutativeAnd => {
            let q1 = "SELECT * FROM t WHERE a > 5 AND b < 10";
            let q2 = "SELECT * FROM t WHERE b < 10 AND a > 5";
            assert_eq!(conn.query(q1), conn.query(q2));
        }
        // ... other relations
    }
}
```

**Expected value:** Catches optimizer bugs where semantically equivalent queries return different results.

---

### 7. Query Plan Verification

**What it is:** Automated verification that Turso's query plans match SQLite's for equivalent queries.

**Implementation:**
```rust
// testing/plan-verifier/src/lib.rs

fn verify_plan_equivalence(sql: &str) {
    let turso_plan = turso_conn.query(&format!("EXPLAIN QUERY PLAN {}", sql));
    let sqlite_plan = sqlite_conn.query(&format!("EXPLAIN QUERY PLAN {}", sql));

    // Parse and compare plan structures
    let turso_ops = parse_plan(&turso_plan);
    let sqlite_ops = parse_plan(&sqlite_plan);

    // Verify same indexes used, same join order, same scan types
    assert_plan_equivalent(turso_ops, sqlite_ops);
}
```

**Expected value:** Ensures query optimizer produces efficient plans matching SQLite's behavior.

---

### 8. Long-Running Soak Tests

**What it is:** Extended duration tests (hours/days) to find memory leaks, resource exhaustion, and performance degradation.

**Implementation:**
```rust
// testing/soak/src/main.rs

fn main() {
    let start = Instant::now();
    let duration = Duration::from_hours(24);
    let conn = Connection::open("soak_test.db").unwrap();

    let mut iteration = 0u64;
    while start.elapsed() < duration {
        // Mixed workload
        perform_random_operations(&conn);

        // Periodic health checks
        if iteration % 10000 == 0 {
            let mem = get_memory_usage();
            let integrity = conn.query("PRAGMA integrity_check").unwrap();

            println!("Iteration {}: mem={}MB, integrity={}",
                     iteration, mem / 1024 / 1024, integrity);

            assert!(mem < MAX_ALLOWED_MEMORY);
            assert_eq!(integrity, "ok");
        }

        iteration += 1;
    }
}
```

**CI Integration:** Run weekly or on release branches.

**Expected value:** Catches slow memory leaks and resource exhaustion that short tests miss.

---

### 9. Regression Database Testing

**What it is:** Testing against real-world database files to ensure compatibility with existing SQLite databases.

**Implementation:**
```bash
# Collect diverse real-world SQLite databases
# (with permission, anonymized if needed)
testing/regression-dbs/
├── chinook.db          # Sample music database
├── northwind.db        # Sample business database
├── large_table.db      # 10M+ rows
├── many_indexes.db     # 100+ indexes
├── unicode_heavy.db    # International text
├── blob_heavy.db       # Large BLOBs
└── schema_complex.db   # Views, triggers, FKs
```

```rust
#[test]
fn test_regression_databases() {
    for db_path in glob("testing/regression-dbs/*.db") {
        let conn = Connection::open(&db_path).unwrap();

        // Basic operations must work
        assert_eq!(conn.query("PRAGMA integrity_check"), Ok("ok"));

        // Query all tables
        let tables = conn.query("SELECT name FROM sqlite_master WHERE type='table'");
        for table in tables {
            conn.query(&format!("SELECT COUNT(*) FROM {}", table)).unwrap();
        }
    }
}
```

**Expected value:** Ensures Turso can open and query real-world SQLite databases correctly.

---

### 10. Formal Verification with TLA+

**What it is:** Formal specification and model checking of critical algorithms (WAL, MVCC, transaction isolation).

**Why it matters:** Database correctness is hard to test exhaustively. Formal verification mathematically proves correctness.

**Implementation approach:**
```tla
--------------------------- MODULE WAL ---------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxTxns, MaxPages

VARIABLES
    wal,           \* Write-ahead log entries
    database,      \* Current database state
    transactions,  \* Active transactions
    checkpointed   \* Last checkpointed position

TypeInvariant ==
    /\ wal \in Seq([txn_id: Nat, page: Nat, data: Nat])
    /\ database \in [1..MaxPages -> Nat]
    /\ transactions \subseteq 1..MaxTxns

\* Safety: Committed data is never lost
Durability ==
    \A txn \in CommittedTransactions:
        DataVisibleAfterRestart(txn)

\* Safety: Uncommitted data is rolled back
Atomicity ==
    \A txn \in AbortedTransactions:
        ~DataVisible(txn)

Spec == Init /\ [][Next]_vars /\ WF_vars(Checkpoint)
-------------------------------------------------------------
```

**Expected value:** Proves transaction properties hold under all interleavings. Found bugs in real systems like Cosmos DB.

---

## Lower-Priority / Long-Term Recommendations

### 11. Coverage-Guided Fuzzing Enhancement

Integrate coverage feedback into the differential oracle to guide test generation toward unexplored code paths.

### 12. SQL Injection Testing

Build a test suite specifically for SQL injection prevention in any parameter handling code.

### 13. Boundary Value Analysis

Systematic testing of edge cases:
- Maximum integer values (i64::MAX, i64::MIN)
- Empty strings, NULL values
- Maximum page size, row size
- Zero-row tables, single-row tables

### 14. Performance Regression Detection

Enhance codspeed integration with:
- Automated bisection on regression
- Statistical significance testing
- Per-query performance tracking

### 15. Chaos Monkey for I/O

Random I/O failures during normal operation:
```rust
struct ChaosIO<T> {
    inner: T,
    fail_probability: f64,
}

impl<T: Read> Read for ChaosIO<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if rand::random::<f64>() < self.fail_probability {
            return Err(io::Error::new(io::ErrorKind::Other, "chaos!"));
        }
        self.inner.read(buf)
    }
}
```

---

## Implementation Priority Matrix

| Enhancement | Effort | Value | Priority |
|-------------|--------|-------|----------|
| Mutation Testing | Low | High | **P0** |
| SQLancer Integration | Medium | Very High | **P0** |
| Miri for Unsafe | Low | High | **P0** |
| Crash Recovery Injection | Medium | Very High | **P1** |
| Property-Based (proptest) | Medium | High | **P1** |
| Metamorphic Testing | Medium | Medium | **P2** |
| Query Plan Verification | Medium | Medium | **P2** |
| Soak Tests | Low | Medium | **P2** |
| Regression DBs | Low | Medium | **P2** |
| TLA+ Formal Verification | High | Very High | **P3** |

---

## Quick Wins (< 1 day each)

1. **Add cargo-mutants to CI** - 2 hours
2. **Run Miri on core crate** - 2 hours
3. **Add proptest dependency and first property** - 4 hours
4. **Collect regression databases** - 2 hours
5. **Add memory tracking to stress tests** - 4 hours

---

## Insights from Industry Database Projects

The following techniques are derived from studying how major database projects approach testing. Each technique includes the source project and specific implementation recommendations for Turso.

---

### From SQLite: The 590:1 Test-to-Code Ratio Standard

SQLite maintains **590 lines of test code for every line of production code**. Key techniques:

#### OOM (Out-of-Memory) Testing

SQLite's modified `malloc()` can be rigged to fail after N allocations, testing memory exhaustion handling.

```rust
// testing/oom/src/lib.rs
use std::sync::atomic::{AtomicUsize, Ordering};

static MALLOC_COUNTDOWN: AtomicUsize = AtomicUsize::new(usize::MAX);
static MALLOC_FAIL_MODE: AtomicBool = AtomicBool::new(false); // true = keep failing

pub fn set_malloc_fail_after(n: usize, persistent: bool) {
    MALLOC_COUNTDOWN.store(n, Ordering::SeqCst);
    MALLOC_FAIL_MODE.store(persistent, Ordering::SeqCst);
}

// Hook into allocator or wrap Vec/Box allocations in tests
pub fn checked_alloc<T>(value: T) -> Result<Box<T>, OomError> {
    let remaining = MALLOC_COUNTDOWN.fetch_sub(1, Ordering::SeqCst);
    if remaining == 0 {
        if !MALLOC_FAIL_MODE.load(Ordering::SeqCst) {
            MALLOC_COUNTDOWN.store(usize::MAX, Ordering::SeqCst); // Reset
        }
        return Err(OomError);
    }
    Ok(Box::new(value))
}

// Test pattern: increment failure point until operation completes
#[test]
fn test_insert_handles_oom() {
    for fail_at in 1..1000 {
        set_malloc_fail_after(fail_at, false);
        let result = perform_insert_operation();

        // Either succeeds or fails gracefully (no corruption)
        if result.is_ok() { break; }

        // Verify no memory leak even after OOM
        assert_no_memory_leaks();
        // Verify database not corrupted
        assert_integrity_check_passes();
    }
}
```

#### I/O Error Injection via VFS

SQLite uses a custom Virtual File System layer that can inject I/O errors after N operations.

```rust
// testing/io-fault/src/vfs.rs
pub struct FaultInjectingVfs {
    inner: Box<dyn Vfs>,
    read_countdown: AtomicUsize,
    write_countdown: AtomicUsize,
}

impl Vfs for FaultInjectingVfs {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.read_countdown.fetch_sub(1, Ordering::SeqCst);
        if remaining == 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "injected read error"));
        }
        self.inner.read(offset, buf)
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<usize> {
        let remaining = self.write_countdown.fetch_sub(1, Ordering::SeqCst);
        if remaining == 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "injected write error"));
        }
        self.inner.write(offset, data)
    }
}
```

#### ALWAYS/NEVER Defense-in-Depth Macros

SQLite uses macros that behave differently in testing vs production:

```rust
// core/src/macros.rs

/// Condition believed to always be true, but with complex proof.
/// - In tests: acts like assert!()
/// - In production: evaluates and continues (defense in depth)
/// - In coverage mode: becomes constant true
#[cfg(test)]
macro_rules! always {
    ($cond:expr) => { assert!($cond, "ALWAYS condition failed: {}", stringify!($cond)) };
}

#[cfg(not(test))]
macro_rules! always {
    ($cond:expr) => { $cond }; // Returns the value for defensive code
}

/// Condition believed to never be true
#[cfg(test)]
macro_rules! never {
    ($cond:expr) => { assert!(!$cond, "NEVER condition triggered: {}", stringify!($cond)) };
}

// Usage in code:
fn process_page(page: &Page) -> Result<()> {
    if never!(page.is_corrupted()) {
        return Err(Error::Corruption); // Defensive, but shouldn't happen
    }
    // ... normal processing
}
```

#### dbsqlfuzz: Simultaneous SQL and Database Mutation

SQLite's most powerful fuzzer mutates **both the SQL query AND the database file** simultaneously. This finds bugs that query-only or file-only fuzzing misses.

```rust
// testing/dual-fuzzer/src/lib.rs
pub struct DualFuzzer {
    sql_mutator: SqlMutator,
    db_mutator: DbFileMutator,
}

impl DualFuzzer {
    pub fn fuzz_iteration(&mut self, seed: u64) {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        // Create base database
        let db_bytes = self.create_valid_database(&mut rng);

        // Mutate database file (corrupt headers, pages, etc.)
        let mutated_db = self.db_mutator.mutate(&db_bytes, &mut rng);

        // Generate SQL that references the mutated schema
        let sql = self.sql_mutator.generate(&mutated_db, &mut rng);

        // Execute and verify no crashes, memory errors
        let result = execute_safely(&mutated_db, &sql);

        // Should either succeed or return clean error
        assert!(result.is_ok() || result.is_clean_error());
    }
}
```

---

### From DuckDB: Extended SQLLogicTest Features

DuckDB significantly extends the sqllogictest format with features Turso could adopt.

#### Query Verification Mode (Optimizer On/Off Comparison)

```sql
-- Add to turso-test-runner
@pragma enable_verification

-- When enabled, every query runs twice:
-- 1. With optimizer enabled
-- 2. With optimizer disabled (cross-product joins)
-- Results must match

@query
SELECT * FROM t1 JOIN t2 ON t1.a = t2.b WHERE t1.x > 5;
@expected
-- Results verified to match between optimized and unoptimized
```

```rust
// turso-test-runner/src/verification.rs
pub fn execute_with_verification(conn: &Connection, sql: &str) -> Result<QueryResult> {
    // Run optimized
    let optimized = conn.execute(sql)?;

    // Disable optimizer
    conn.execute("PRAGMA query_only = 1")?; // or equivalent
    conn.execute("PRAGMA optimizer_enabled = 0")?;

    let unoptimized = conn.execute(sql)?;

    // Re-enable
    conn.execute("PRAGMA optimizer_enabled = 1")?;

    // Compare (order-independent for unordered queries)
    if !results_equivalent(&optimized, &unoptimized) {
        return Err(VerificationError::ResultMismatch {
            optimized,
            unoptimized,
            sql: sql.to_string(),
        });
    }

    Ok(optimized)
}
```

#### Automatic Test Case Minimization

DuckDB's `reduce_sql_statement()` function automatically generates simplified candidate queries.

```rust
// turso-test-runner/src/reducer.rs
pub fn reduce_sql_statement(sql: &str) -> Vec<String> {
    let ast = parse_sql(sql).unwrap();
    let mut candidates = Vec::new();

    // Strategy 1: Remove columns from SELECT
    if let Some(select) = ast.as_select() {
        for i in 0..select.columns.len() {
            let mut reduced = select.clone();
            reduced.columns.remove(i);
            candidates.push(reduced.to_sql());
        }
    }

    // Strategy 2: Remove conditions from WHERE
    if let Some(where_clause) = ast.where_clause() {
        for subcondition in where_clause.iter_conditions() {
            let reduced = ast.without_condition(subcondition);
            candidates.push(reduced.to_sql());
        }
    }

    // Strategy 3: Remove JOINs
    // Strategy 4: Remove subqueries
    // Strategy 5: Simplify expressions

    candidates
}

// Usage in debugging:
fn minimize_failing_query(original_sql: &str, error: &Error) -> String {
    let mut current = original_sql.to_string();

    loop {
        let candidates = reduce_sql_statement(&current);
        let mut found_smaller = false;

        for candidate in candidates {
            if reproduces_error(&candidate, error) {
                current = candidate;
                found_smaller = true;
                break;
            }
        }

        if !found_smaller { break; }
    }

    current // Minimal reproduction
}
```

#### Concurrent Loop Testing in SQLTest

```sql
-- Extension to .sqltest format
@database :memory:

@setup
CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER);
INSERT INTO accounts VALUES (1, 1000), (2, 1000);

-- Run these concurrently on separate connections
@concurrentloop i 1 10
@connection con_${i}
BEGIN;
UPDATE accounts SET balance = balance - 10 WHERE id = 1;
UPDATE accounts SET balance = balance + 10 WHERE id = 2;
COMMIT;
@endloop

-- Verify final state
@query
SELECT SUM(balance) FROM accounts;
@expected
2000
```

#### Automated Fuzzer Issue Repository

DuckDB maintains a separate `duckdb-fuzzer` repository with automated issue management:

```yaml
# .github/workflows/fuzzer-issues.yml
name: Fuzzer Issue Management

on:
  schedule:
    - cron: '0 */4 * * *'  # Every 4 hours

jobs:
  run-fuzzers:
    runs-on: ubuntu-latest
    steps:
      - name: Run SQLsmith
        run: cargo run -p sqlsmith -- --iterations 10000

      - name: Check for failures
        id: check
        run: |
          if [ -f "fuzzer_failures.log" ]; then
            echo "has_failures=true" >> $GITHUB_OUTPUT
          fi

      - name: Create/Update Issues
        if: steps.check.outputs.has_failures == 'true'
        uses: actions/github-script@v6
        with:
          script: |
            const failures = require('./fuzzer_failures.json');
            for (const failure of failures) {
              // Check for duplicate by error message
              const existing = await findExistingIssue(failure.error_hash);
              if (!existing) {
                await createIssue(failure);
              }
            }

  close-fixed-issues:
    runs-on: ubuntu-latest
    steps:
      - name: Re-test open issues
        run: |
          # Fetch all open fuzzer issues
          # Re-run their reproduction cases
          # Close issues that no longer reproduce
```

---

### From CockroachDB: 80K Tests Per PR

#### Metamorphic Testing Across Configurations

CockroachDB runs the same operations against different storage configurations and compares results.

```rust
// testing/metamorphic/src/lib.rs
use rand::prelude::*;

pub struct MetamorphicConfig {
    pub page_size: usize,
    pub cache_size: usize,
    pub journal_mode: JournalMode,
    pub synchronous: SyncMode,
    pub wal_autocheckpoint: u32,
    // ... many more knobs
}

impl MetamorphicConfig {
    pub fn randomize(seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        Self {
            page_size: [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
                .choose(&mut rng).copied().unwrap(),
            cache_size: rng.gen_range(10..100000),
            journal_mode: [JournalMode::Delete, JournalMode::Truncate,
                          JournalMode::Persist, JournalMode::Wal]
                .choose(&mut rng).copied().unwrap(),
            synchronous: [SyncMode::Off, SyncMode::Normal, SyncMode::Full]
                .choose(&mut rng).copied().unwrap(),
            wal_autocheckpoint: rng.gen_range(0..10000),
        }
    }
}

#[test]
fn metamorphic_storage_test() {
    let seed = std::env::var("SEED").unwrap_or("42".into()).parse().unwrap();

    // Run same operations with two different configurations
    let config1 = MetamorphicConfig::randomize(seed);
    let config2 = MetamorphicConfig::randomize(seed + 1);

    let ops = generate_operations(seed + 2);

    let result1 = run_operations_with_config(&ops, &config1);
    let result2 = run_operations_with_config(&ops, &config2);

    // Results must be identical despite different configs
    assert_eq!(result1, result2,
        "Metamorphic test failed!\nSeed: {}\nConfig1: {:?}\nConfig2: {:?}",
        seed, config1, config2);
}
```

#### Cross-Version Compatibility Testing

```rust
// testing/version-compat/src/lib.rs

/// Tests that databases created by older versions can be read by newer versions
#[test]
fn test_forward_compatibility() {
    // Database created by Turso 0.4.x
    let old_db = include_bytes!("../fixtures/v0.4_database.db");

    // Current version should read it
    let conn = Connection::open_from_bytes(old_db).unwrap();
    assert_eq!(conn.query("PRAGMA integrity_check"), Ok("ok".into()));

    // All data should be accessible
    let rows: Vec<Row> = conn.query("SELECT * FROM test_table").unwrap();
    assert_eq!(rows.len(), EXPECTED_ROW_COUNT);
}

/// Tests that databases created by current version have expected format
#[test]
fn test_format_stability() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES(42)").unwrap();

    let db_bytes = conn.serialize();

    // Verify header format
    assert_eq!(&db_bytes[0..16], b"SQLite format 3\0");
    // Verify page size matches expected
    let page_size = u16::from_be_bytes([db_bytes[16], db_bytes[17]]);
    assert!(page_size >= 512 && page_size <= 65536);
}
```

---

### From FoundationDB: BUGGIFY Pattern

FoundationDB's BUGGIFY macro is sprinkled throughout production code, only activating in simulation.

```rust
// core/src/buggify.rs
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use rand::prelude::*;

static SIMULATION_MODE: AtomicBool = AtomicBool::new(false);
static SIMULATION_SEED: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static BUGGIFY_RNG: RefCell<Option<ChaCha8Rng>> = RefCell::new(None);
    static ENABLED_BUGGIFY_POINTS: RefCell<HashSet<&'static str>> = RefCell::new(HashSet::new());
}

pub fn enable_simulation(seed: u64) {
    SIMULATION_MODE.store(true, Ordering::SeqCst);
    SIMULATION_SEED.store(seed, Ordering::SeqCst);

    // Deterministically decide which BUGGIFY points are enabled for this run
    BUGGIFY_RNG.with(|rng| {
        *rng.borrow_mut() = Some(ChaCha8Rng::seed_from_u64(seed));
    });
}

/// Returns true with given probability, but ONLY in simulation mode.
/// The first call for each location determines if it's enabled for the entire run.
#[macro_export]
macro_rules! buggify {
    () => { buggify!(0.25) };
    ($prob:expr) => {{
        if $crate::buggify::in_simulation() {
            $crate::buggify::should_fire(concat!(file!(), ":", line!()), $prob)
        } else {
            false
        }
    }};
}

pub fn should_fire(location: &'static str, probability: f64) -> bool {
    BUGGIFY_RNG.with(|rng| {
        let mut rng = rng.borrow_mut();
        let rng = rng.as_mut().unwrap();

        ENABLED_BUGGIFY_POINTS.with(|points| {
            let mut points = points.borrow_mut();

            // First call at this location: decide if enabled for entire run
            if !points.contains(location) {
                if rng.gen_bool(0.5) { // 50% of points enabled per run
                    points.insert(location);
                }
            }

            // If enabled, fire with given probability
            if points.contains(location) {
                rng.gen_bool(probability)
            } else {
                false
            }
        })
    })
}

// Usage throughout codebase:
fn write_page(&mut self, page_num: u32, data: &[u8]) -> io::Result<()> {
    // Simulate slow disk
    if buggify!(0.1) {
        std::thread::sleep(Duration::from_millis(100));
    }

    // Simulate partial write
    if buggify!(0.01) {
        let partial_len = self.rng.gen_range(0..data.len());
        self.file.write(&data[..partial_len])?;
        return Err(io::Error::new(io::ErrorKind::Other, "simulated partial write"));
    }

    // Simulate write reordering (write to wrong offset)
    if buggify!(0.001) {
        let wrong_offset = self.rng.gen_range(0..self.file_size);
        self.file.seek(SeekFrom::Start(wrong_offset))?;
    }

    self.file.write_all(data)
}
```

#### Swizzle-Clogging Failure Pattern

```rust
// testing/chaos/src/swizzle_clog.rs

/// Swizzle-clogging: The most effective failure pattern from FoundationDB.
/// 1. Pick random subset of connections
/// 2. "Clog" (pause) each one by one over several seconds
/// 3. Unclog in RANDOM order (not reverse order!)
pub fn swizzle_clog<S: Simulator>(sim: &mut S, intensity: f64) {
    let connections = sim.get_connections();
    let subset_size = (connections.len() as f64 * intensity) as usize;

    let mut selected: Vec<_> = connections.choose_multiple(&mut sim.rng(), subset_size).collect();

    // Clog phase: pause connections one by one with delays
    for conn in &selected {
        sim.clog_connection(*conn);
        sim.advance_time(Duration::from_millis(sim.rng().gen_range(100..500)));
    }

    // Unclog phase: resume in RANDOM order (key insight!)
    selected.shuffle(&mut sim.rng());
    for conn in selected {
        sim.unclog_connection(conn);
        sim.advance_time(Duration::from_millis(sim.rng().gen_range(100..500)));
    }
}
```

---

### From TiDB: Chaos Mesh and Failpoints

#### Failpoint Injection Library

```rust
// failpoint/src/lib.rs
use std::collections::HashMap;
use std::sync::RwLock;

lazy_static! {
    static ref FAILPOINTS: RwLock<HashMap<String, FailpointAction>> = RwLock::new(HashMap::new());
}

pub enum FailpointAction {
    Return(String),      // Return early with value
    Panic(String),       // Panic with message
    Sleep(Duration),     // Sleep for duration
    Print(String),       // Print and continue
    Pause,               // Pause until unpaused
    Yield,               // Yield to other threads
    Delay(Duration),     // Delay then continue
    Off,                 // Disabled
}

/// Set failpoint via environment variable or programmatically
/// Format: "name=action(args)"
/// Example: "wal_write=sleep(100ms)" or "commit=return(error)"
pub fn enable(name: &str, action: FailpointAction) {
    FAILPOINTS.write().unwrap().insert(name.to_string(), action);
}

#[macro_export]
macro_rules! fail_point {
    ($name:expr) => {
        if let Some(action) = $crate::failpoint::get_action($name) {
            match action {
                FailpointAction::Return(v) => return Err(Error::Injected(v)),
                FailpointAction::Panic(msg) => panic!("failpoint {}: {}", $name, msg),
                FailpointAction::Sleep(d) => std::thread::sleep(d),
                FailpointAction::Pause => $crate::failpoint::wait_for_unpause($name),
                FailpointAction::Delay(d) => std::thread::sleep(d),
                _ => {}
            }
        }
    };
}

// Usage in code:
fn commit_transaction(&mut self) -> Result<()> {
    fail_point!("before_wal_write");

    self.write_wal()?;

    fail_point!("after_wal_before_commit");

    self.update_database()?;

    fail_point!("after_commit");

    Ok(())
}

// In tests:
#[test]
fn test_crash_after_wal_write() {
    enable("after_wal_before_commit", FailpointAction::Panic("simulated crash".into()));

    let result = std::panic::catch_unwind(|| {
        conn.execute("INSERT INTO t VALUES(1)").unwrap();
    });

    assert!(result.is_err());

    // Reopen and verify WAL recovery
    let conn2 = Connection::open(db_path).unwrap();
    // Data should either be there (committed) or not (rolled back)
    // but database should be consistent
}
```

---

### From PostgreSQL: Isolation Testing

#### All-Permutations Concurrency Testing

PostgreSQL's `isolationtester` runs all possible interleavings of concurrent operations.

```rust
// testing/isolation/src/lib.rs

#[derive(Debug)]
pub struct IsolationTest {
    pub setup: String,
    pub sessions: Vec<Session>,
    pub permutations: Option<Vec<Vec<usize>>>, // If None, test all permutations
}

#[derive(Debug)]
pub struct Session {
    pub name: String,
    pub setup: String,
    pub steps: Vec<Step>,
    pub teardown: String,
}

#[derive(Debug)]
pub struct Step {
    pub name: String,
    pub sql: String,
    pub blocking: bool, // This step may block waiting for locks
}

pub fn run_isolation_test(test: &IsolationTest) -> Vec<PermutationResult> {
    let all_steps: Vec<(usize, usize)> = test.sessions.iter()
        .enumerate()
        .flat_map(|(sess_idx, sess)| {
            (0..sess.steps.len()).map(move |step_idx| (sess_idx, step_idx))
        })
        .collect();

    let permutations = test.permutations.clone()
        .unwrap_or_else(|| generate_all_permutations(&all_steps, &test.sessions));

    let mut results = Vec::new();

    for perm in permutations {
        let result = run_permutation(test, &perm);
        results.push(result);
    }

    results
}

fn run_permutation(test: &IsolationTest, order: &[usize]) -> PermutationResult {
    // Create connections for each session
    let connections: Vec<_> = test.sessions.iter()
        .map(|_| Connection::open_in_memory().unwrap())
        .collect();

    // Run setup
    for (conn, sess) in connections.iter().zip(&test.sessions) {
        conn.execute(&sess.setup).unwrap();
    }

    // Execute steps in specified order, handling blocking
    let mut output = Vec::new();
    for &step_idx in order {
        let (sess_idx, local_step_idx) = decode_step_index(step_idx);
        let step = &test.sessions[sess_idx].steps[local_step_idx];

        // Execute with timeout for blocking detection
        let result = execute_with_timeout(&connections[sess_idx], &step.sql, TIMEOUT);
        output.push((step.name.clone(), result));
    }

    PermutationResult { order: order.to_vec(), output }
}

// Example test specification (could be in a DSL file)
/*
setup {
    CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);
    INSERT INTO accounts VALUES (1, 100), (2, 100);
}

session s1 {
    setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
    step s1_read { SELECT * FROM accounts WHERE id = 1; }
    step s1_write { UPDATE accounts SET balance = 50 WHERE id = 1; }
    teardown { COMMIT; }
}

session s2 {
    setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
    step s2_read { SELECT * FROM accounts WHERE id = 1; }
    step s2_write { UPDATE accounts SET balance = 75 WHERE id = 1; }
    teardown { COMMIT; }
}

# Test all interleavings automatically
*/
```

---

### From RocksDB: Expected State / Shadow Database

#### Shadow Database for Validation

```rust
// testing/expected-state/src/lib.rs

/// Maintains an in-memory model of what the database should contain.
/// Every operation is mirrored here, and reads are validated against it.
pub struct ExpectedState {
    // Map from key to (value, exists, sequence_number)
    state: HashMap<Vec<u8>, (Vec<u8>, bool, u64)>,
    sequence: u64,

    // For crash recovery testing
    checkpointed_state: HashMap<Vec<u8>, (Vec<u8>, bool, u64)>,
    operation_trace: Vec<Operation>,
}

#[derive(Clone, Debug)]
pub enum Operation {
    Put { key: Vec<u8>, value: Vec<u8>, seq: u64 },
    Delete { key: Vec<u8>, seq: u64 },
    Checkpoint { seq: u64 },
}

impl ExpectedState {
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.sequence += 1;
        self.state.insert(key.to_vec(), (value.to_vec(), true, self.sequence));
        self.operation_trace.push(Operation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            seq: self.sequence,
        });
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.sequence += 1;
        if let Some(entry) = self.state.get_mut(key) {
            entry.1 = false;
            entry.2 = self.sequence;
        }
        self.operation_trace.push(Operation::Delete {
            key: key.to_vec(),
            seq: self.sequence,
        });
    }

    pub fn checkpoint(&mut self) {
        self.checkpointed_state = self.state.clone();
        self.operation_trace.push(Operation::Checkpoint { seq: self.sequence });
        self.operation_trace.clear(); // Only keep ops after checkpoint
    }

    /// Verify actual database matches expected state
    pub fn verify(&self, conn: &Connection) -> Result<(), VerifyError> {
        for (key, (expected_value, exists, _)) in &self.state {
            let actual = conn.get(key)?;

            match (exists, actual) {
                (true, Some(v)) if v == *expected_value => continue,
                (true, Some(v)) => return Err(VerifyError::ValueMismatch {
                    key: key.clone(),
                    expected: expected_value.clone(),
                    actual: v,
                }),
                (true, None) => return Err(VerifyError::MissingKey { key: key.clone() }),
                (false, Some(v)) => return Err(VerifyError::UnexpectedKey {
                    key: key.clone(),
                    value: v,
                }),
                (false, None) => continue,
            }
        }
        Ok(())
    }

    /// After crash, reconstruct expected state up to recovered sequence number
    pub fn reconstruct_for_recovery(&self, recovered_seq: u64) -> ExpectedState {
        let mut state = self.checkpointed_state.clone();

        for op in &self.operation_trace {
            match op {
                Operation::Put { key, value, seq } if *seq <= recovered_seq => {
                    state.insert(key.clone(), (value.clone(), true, *seq));
                }
                Operation::Delete { key, seq } if *seq <= recovered_seq => {
                    if let Some(entry) = state.get_mut(key) {
                        entry.1 = false;
                        entry.2 = *seq;
                    }
                }
                _ => break, // Stop at ops beyond recovered sequence
            }
        }

        ExpectedState {
            state,
            sequence: recovered_seq,
            checkpointed_state: self.checkpointed_state.clone(),
            operation_trace: Vec::new(),
        }
    }
}
```

#### White-Box Crash Points

```rust
// core/src/crash_points.rs

#[cfg(feature = "crash-testing")]
pub static CRASH_POINTS: Lazy<DashMap<&'static str, CrashConfig>> = Lazy::new(DashMap::new);

#[cfg(feature = "crash-testing")]
pub struct CrashConfig {
    pub probability: f64,
    pub countdown: AtomicUsize,
}

#[cfg(feature = "crash-testing")]
macro_rules! crash_point {
    ($name:expr) => {
        if let Some(config) = CRASH_POINTS.get($name) {
            let countdown = config.countdown.fetch_sub(1, Ordering::SeqCst);
            if countdown == 1 || rand::random::<f64>() < config.probability {
                std::process::abort(); // Simulate crash
            }
        }
    };
}

#[cfg(not(feature = "crash-testing"))]
macro_rules! crash_point {
    ($name:expr) => {};
}

// Usage in WAL code:
fn write_wal_record(&mut self, record: &WalRecord) -> io::Result<()> {
    crash_point!("before_wal_write");

    let bytes = record.serialize();
    self.wal_file.write_all(&bytes)?;

    crash_point!("after_wal_write_before_sync");

    self.wal_file.sync_all()?;

    crash_point!("after_wal_sync");

    Ok(())
}

// In checkpoint code:
fn checkpoint(&mut self) -> io::Result<()> {
    crash_point!("before_checkpoint_start");

    for page in self.dirty_pages() {
        crash_point!("during_checkpoint_page_write");
        self.write_page(page)?;
    }

    crash_point!("after_checkpoint_before_header_update");

    self.update_checkpoint_header()?;

    crash_point!("after_checkpoint_complete");

    Ok(())
}
```

---

## Updated Implementation Priority Matrix

| Enhancement | Source | Effort | Value | Priority |
|-------------|--------|--------|-------|----------|
| BUGGIFY macros throughout codebase | FoundationDB | Medium | Very High | **P0** |
| Expected State / Shadow DB | RocksDB | Medium | Very High | **P0** |
| Query Verification Mode (opt on/off) | DuckDB | Low | High | **P0** |
| OOM Testing | SQLite | Medium | High | **P1** |
| I/O Error Injection VFS | SQLite | Medium | High | **P1** |
| White-Box Crash Points | RocksDB | Low | High | **P1** |
| Isolation Testing (all permutations) | PostgreSQL | High | Very High | **P1** |
| Test Case Minimizer | DuckDB | Medium | Medium | **P2** |
| Failpoint Library | TiDB | Medium | Medium | **P2** |
| Metamorphic Config Testing | CockroachDB | Medium | High | **P2** |
| Cross-Version Compatibility | CockroachDB | Low | Medium | **P2** |
| Swizzle-Clogging | FoundationDB | Low | Medium | **P2** |
| Dual SQL+DB Fuzzer | SQLite | High | Very High | **P3** |
| Automated Fuzzer Issue Repo | DuckDB | Medium | Medium | **P3** |

---

## Quick Wins from Industry Research

1. **Add BUGGIFY macro** - 4 hours - Sprinkle throughout I/O and transaction code
2. **Query verification mode** - 4 hours - Run queries twice (optimized/unoptimized)
3. **Add crash points** - 2 hours - Insert `crash_point!()` at critical locations
4. **Expected state tracker** - 6 hours - Build shadow DB for stress tests
5. **Cross-version test fixtures** - 2 hours - Save databases from older versions

---

## Resources

- [cargo-mutants](https://github.com/sourcefrog/cargo-mutants) - Mutation testing
- [SQLancer](https://github.com/sqlancer/sqlancer) - Database logic bug finder
- [Miri](https://github.com/rust-lang/miri) - Undefined behavior detector
- [proptest](https://github.com/proptest-rs/proptest) - Property-based testing
- [TLA+](https://lamport.azurewebsites.net/tla/tla.html) - Formal verification
- [Jepsen](https://jepsen.io/) - Distributed systems testing (inspiration)

### Database-Specific Resources

- [How SQLite Is Tested](https://sqlite.org/testing.html) - 590:1 test ratio methodology
- [DuckDB Testing Documentation](https://duckdb.org/docs/dev/sqllogictest/overview) - Extended sqllogictest
- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html) - Deterministic simulation
- [CockroachDB Metamorphic Testing](https://www.cockroachlabs.com/blog/metamorphic-testing-the-database/) - Configuration randomization
- [RocksDB Stress Testing](https://github.com/facebook/rocksdb/wiki/Stress-test) - Expected state pattern
- [TiDB Chaos Mesh](https://chaos-mesh.org/) - Kubernetes chaos engineering
- [PostgreSQL Isolation Tests](https://github.com/postgres/postgres/tree/master/src/test/isolation) - Permutation testing
