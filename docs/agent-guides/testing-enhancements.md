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

## Resources

- [cargo-mutants](https://github.com/sourcefrog/cargo-mutants) - Mutation testing
- [SQLancer](https://github.com/sqlancer/sqlancer) - Database logic bug finder
- [Miri](https://github.com/rust-lang/miri) - Undefined behavior detector
- [proptest](https://github.com/proptest-rs/proptest) - Property-based testing
- [TLA+](https://lamport.azurewebsites.net/tla/tla.html) - Formal verification
- [Jepsen](https://jepsen.io/) - Distributed systems testing (inspiration)
