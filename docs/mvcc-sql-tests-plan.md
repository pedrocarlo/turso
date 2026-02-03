# MVCC SQL Tests Compatibility Plan

This document tracks all `.sqltest` files and tests that currently do not run with MVCC mode enabled.

**Total sqltest files:** 74
**Files with MVCC skip directives:** 15
**Total skipped tests:** 65 (7 entire files + 58 individual tests)

---

## Summary by Feature

| Feature Category | File-Level Skips | Test-Level Skips | Priority |
|------------------|------------------|------------------|----------|
| Views | 1 file | 4 tests | High |
| Changes Function | 0 | 14 tests | High |
| Total Changes Function | 1 file | 0 | High |
| Foreign Keys | 1 file | 0 | Medium |
| Expression Indexes | 0 | 9 tests | Medium |
| Partial Indexes | 1 file | 3 tests | Medium |
| Pragma Functions | 0 | 21 tests | Medium |
| Integrity Check | 1 file | 0 | Low |
| Cursor ID Differences | 2 files | 0 | Low |
| Database Locking | 0 | 1 test | High |
| Autoincrement | 0 | 1 test | Medium |

---

## 1. Views Support

**Reason:** "views not supported in MVCC mode"
**Impact:** 1 file + 4 tests
**Files affected:** `views.sqltest`, `create_table.sqltest`

### Tasks

- [ ] Investigate why views don't work with MVCC
- [ ] Implement view support in MVCC storage layer
- [ ] Enable `views.sqltest` for MVCC

**File-level skip:**
- [ ] `testing/runner/tests/views.sqltest` - entire file

**Test-level skips in `create_table.sqltest`:**
- [ ] `create_table_view_collision-1` (line 105)
- [ ] `create_view_table_collision-1` (line 113)
- [ ] `create_index_view_collision-1` (line 121)
- [ ] `create_view_index_collision-1` (line 130)

---

## 2. Changes Function

**Reason:** "changes not supported in MVCC mode"
**Impact:** 14 tests
**Files affected:** `changes.sqltest`

### Tasks

- [ ] Implement `changes()` function tracking in MVCC mode
- [ ] Track row modifications per statement in `MvStore`
- [ ] Enable all changes tests for MVCC

**Test-level skips in `changes.sqltest`:**
- [ ] `changes-on-basic-insert` (line 4)
- [ ] `changes-on-multiple-row-insert` (line 14)
- [ ] `changes-shows-most-recent` (line 24)
- [ ] `changes-on-successful-upsert` (line 35) - issue #3259
- [ ] `changes-on-replace-single-conflict` (line 49) - issue #3688
- [ ] `changes-on-delete` (line 63)
- [ ] `changes-on-update` (line 74)
- [ ] `changes-on-update-rowid` (line 85)
- [ ] `changes-resets-after-select` (line 96)
- [ ] `changes-on-delete-no-match` (line 110)
- [ ] `changes-on-update-no-match` (line 121)
- [ ] `changes-on-delete-all` (line 132)
- [ ] `changes-mixed-operations` (line 143)

---

## 3. Total Changes Function

**Reason:** "total_changes not supported in MVCC mode"
**Impact:** 1 file
**Files affected:** `total-changes.sqltest`

### Tasks

- [ ] Implement `total_changes()` function tracking in MVCC mode
- [ ] Maintain cumulative modification counter across connection lifetime
- [ ] Enable `total-changes.sqltest` for MVCC

**File-level skip:**
- [ ] `testing/runner/tests/total-changes.sqltest` - entire file

---

## 4. Foreign Keys

**Reason:** "foreign keys not supported with MVCC"
**Impact:** 1 file
**Files affected:** `foreign_keys.sqltest`

### Tasks

- [ ] Investigate foreign key constraint checking with MVCC versioned rows
- [ ] Implement FK validation that considers row visibility
- [ ] Handle cascading deletes/updates with MVCC
- [ ] Enable `foreign_keys.sqltest` for MVCC

**File-level skip:**
- [ ] `testing/runner/tests/foreign_keys.sqltest` - entire file

---

## 5. Expression Indexes

**Reason:** "Expression indexes are not currently enabled in MVCC"
**Impact:** 9 tests
**Files affected:** `create_index.sqltest`

### Tasks

- [ ] Enable expression index creation in MVCC mode
- [ ] Ensure expression indexes update correctly with row versions
- [ ] Enable all expression index tests for MVCC

**Test-level skips in `create_index.sqltest`:**
- [ ] `create-index-string-literal-error` (line 136)
- [ ] `create-index-string-literal-column-name` (line 146)
- [ ] `create-index-numeric-literal` (line 157)
- [ ] `create-index-pure-numeric-expression` (line 168)
- [ ] `create-index-expression-with-column` (line 179)
- [ ] `create-index-multi-column-string-literal-error` (line 190)
- [ ] `create-index-string-in-expression` (line 200)
- [ ] `create-index-deep-paren-string-column` (line 211)
- [ ] `create-index-deep-paren-string-error` (line 222)

---

## 6. Partial Indexes

**Reason:** "partial indexes not fully supported in MVCC mode"
**Impact:** 1 file + 3 tests
**Files affected:** `partial_idx.sqltest`, `insert.sqltest`

### Tasks

- [ ] Investigate partial index support with MVCC
- [ ] Ensure WHERE clause evaluation works with row versions
- [ ] Handle index entry removal when row no longer matches predicate
- [ ] Enable partial index tests for MVCC

**File-level skip:**
- [ ] `testing/runner/tests/partial_idx.sqltest` - entire file

**Test-level skips in `insert.sqltest`:**
- [ ] `partial-expr-index-upsert-conflict` (line 1409)
- [ ] `partial-expr-index-update-removes-entry` (line 1426)
- [ ] `partial-expr-index-delete-clears-entry` (line 1445)

---

## 7. Pragma Functions

**Reasons:** Various pragma implementations missing in MVCC
**Impact:** 21 tests
**Files affected:** `pragma/default.sqltest`, `pragma/memory.sqltest`

### 7.1 Page Count Pragma

- [ ] Implement `page_count` pragma for MVCC
- [ ] `pragma-page-count-table` (memory.sqltest:61)
- [ ] `pragma-page-count-empty-2` (memory.sqltest:128)

### 7.2 Max Page Count Pragma

- [ ] Implement `max_page_count` pragma for MVCC
- [ ] `pragma-max-page-count-clamping-with-data` (memory.sqltest:102)
- [ ] `pragma-max-page-count-enforcement-error` (memory.sqltest:113)

### 7.3 User Version Pragma

- [ ] Implement `user_version` pragma read/write for MVCC
- [ ] `pragma-user-version-update` (memory.sqltest:144)
- [ ] `pragma-user-version-negative-value` (memory.sqltest:153)
- [ ] `pragma-user-version-float-value` (memory.sqltest:162)

### 7.4 Application ID Pragma

- [ ] Implement `application_id` pragma for MVCC
- [ ] `pragma-application-id-update` (memory.sqltest:178)
- [ ] `pragma-application-id-float-value` (memory.sqltest:187)
- [ ] `pragma-application-id-large-value` (memory.sqltest:196)
- [ ] `pragma-application-id-negative-value` (memory.sqltest:205)

### 7.5 Journal Mode Pragma

- [ ] Fix journal mode return value for MVCC
- [ ] `pragma-function-update-journal-mode` (memory.sqltest:37)

### 7.6 Pragma Virtual Table Joins (Panic)

**Reason:** "panic: transaction should exist in txs map"

- [ ] Fix transaction tracking bug in pragma virtual table joins
- [ ] `pragma-vtab-join` (default.sqltest:182)
- [ ] `pragma-vtab-reversed-join-order` (default.sqltest:202)

---

## 8. Integrity Check

**Reason:** "not supported in MVCC mode"
**Impact:** 1 file
**Files affected:** `integrity_check/memory.sqltest`

### Tasks

- [ ] Implement `PRAGMA integrity_check` for MVCC
- [ ] Verify both B-tree and version store consistency
- [ ] Enable `integrity_check/memory.sqltest` for MVCC

**File-level skip:**
- [ ] `testing/runner/tests/integrity_check/memory.sqltest` - entire file

---

## 9. Cursor ID Differences (Snapshot Tests)

**Reason:** "mvcc has slightly different cursor ids"
**Impact:** 2 files
**Files affected:** `orderby/orderby_plan.sqltest`, `snapshot_tests/tpch/tpch.sqltest`

### Tasks

- [ ] Investigate cursor ID allocation differences in MVCC
- [ ] Either normalize cursor IDs or create MVCC-specific expected outputs
- [ ] Enable plan snapshot tests for MVCC

**File-level skips:**
- [ ] `testing/runner/tests/orderby/orderby_plan.sqltest`
- [ ] `testing/runner/tests/snapshot_tests/tpch/tpch.sqltest`

---

## 10. Database Locking

**Reason:** "database is locked error"
**Impact:** 1 test
**Files affected:** `orderby/default.sqltest`

### Tasks

- [ ] Investigate database locking issue in MVCC with this specific query
- [ ] Fix concurrent access handling
- [ ] Enable `order-by-column-number-3` test for MVCC

**Test-level skip:**
- [ ] `order-by-column-number-3` (orderby/default.sqltest:127)

---

## 11. Autoincrement with Conflict

**Reason:** "not supported in MVCC mode"
**Impact:** 1 test
**Files affected:** `autoincr.sqltest`

### Tasks

- [ ] Investigate autoincrement with ON CONFLICT DO NOTHING in MVCC
- [ ] Ensure autoincrement counter updates correctly with versioned inserts
- [ ] Enable `autoinc-conflict-on-nothing` test for MVCC

**Test-level skip:**
- [ ] `autoinc-conflict-on-nothing` (autoincr.sqltest:228)

---

## Priority Recommendations

### High Priority (Core Functionality)
1. **Changes/Total Changes Functions** - Essential for application compatibility
2. **Database Locking Bug** - Indicates potential concurrency issue
3. **Views Support** - Common SQL feature

### Medium Priority (Feature Completeness)
4. **Foreign Keys** - Important for data integrity
5. **Partial/Expression Indexes** - Performance optimization features
6. **Pragma Functions** - Application metadata management
7. **Autoincrement** - Common pattern

### Low Priority (Test Infrastructure)
8. **Cursor ID Differences** - Only affects test snapshots, not functionality
9. **Integrity Check** - Diagnostic feature

---

## How to Run MVCC Tests

```bash
# Run all tests with MVCC enabled
make test-mvcc

# Run specific test file with MVCC
cargo run -q --bin test-runner -- testing/runner/tests/changes.sqltest --mvcc

# Run with verbose output
cargo run -q --bin test-runner -- testing/runner/tests/changes.sqltest --mvcc -v
```

## How to Remove a Skip Directive

1. Fix the underlying issue in the MVCC implementation
2. Remove the `@skip-if mvcc` or `@skip-file-if mvcc` directive from the test
3. Run the test with `--mvcc` flag to verify it passes
4. Submit a PR with both the fix and the test enablement
