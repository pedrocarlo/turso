# HIR binding migration

## Goal

Replace the planner-era semantic binder with an analyzer that produces closed,
validated HIR directly. Preserve SQLite binding rules, diagnostics, and test
coverage while replacing old data structures.

Do not make `BindScope` own a `HirDocument`. One `Analyzer` owns the document
arenas. Temporary scopes contain document-local IDs and small lookup facts.
They do not contain planner nodes, mutable AST, runtime registers, or cloned
documents.

## Design mapping

| Old binding concept | HIR design |
|---|---|
| `BindContext` | `Analyzer` owning document arenas |
| `BindScope` | Temporary name-resolution structure |
| `ScopeTable` / `TableInternalId` | `ScopeSource` / `SourceId` |
| Bound alias expression | `OutputId` plus resolved expression facts |
| `BoundSelect` / `BoundSubquery` | `Query`, `QueryBlock`, and `Source` |
| `BindTracking` | Required-column worklist; calculate captures afterward |
| Cloned CTE AST | Lazy CTE state with reserved `CteId` |
| `allow_unbound` | Register sources before resolving expressions |
| `right_join_swapped` | Preserve RIGHT/FULL joins in HIR |
| Trigger registers | NEW/OLD/EXCLUDED pseudo-sources |
| `ProgramBuilder` labels/registers | Physical lowering only |

## Migration order

1. Port `SemanticContext`, analyzer arenas, HIR scope, and expression analysis.
2. Move SELECT vertically: sources -> expressions -> joins -> outputs -> compounds.
3. Add lazy CTE handling and correlation calculation.
4. Move INSERT, UPDATE, DELETE, and pseudo-sources.
5. Move triggers and stored schema expressions.
6. Validate every completed document.
7. Switch the production entry point after coverage is complete.
8. Delete `Bound*`, `TableReferences` conversion, and planner-era binding code.

## Current status

Phase 1 and the early SELECT path are in progress.

Completed:

- Analyzer-owned HIR arenas and mandatory document validation.
- HIR scope with source, output-alias, database-qualified, outer-scope, rowid,
  USING/NATURAL merged-column, star-expansion, and collation rules.
- One ordinary table source and basic result outputs, including stars.
- Immutable AST-to-HIR binding for names, literals, unary and binary operators,
  null tests, parentheses, `COLLATE`, and SQL parameters.
- Iterative expression analysis, including deep-expression coverage.
- `BETWEEN`, list `IN`, `CASE`, and built-in `CAST` expressions.
- Catalog-resolved custom `CAST` targets that need no stored program.
- Simple custom `CAST` encoders bound against document-owned synthetic inputs.
- Domain `NOT NULL` and `CHECK` rules frozen into custom `CAST` targets.
- Scalar function calls with resolved catalog identity, including calls inside
  custom `CAST` encoders.
- Plain aggregate calls with stable query-block identities and frozen result
  type facts. `COUNT(*)`, `DISTINCT`, argument `ORDER BY`, aggregate `FILTER`,
  and ordered-set forms are explicit in HIR; nested aggregates are rejected
  during analysis.
- Inline window calls with stable query-block identities, resolved partition
  and ordering expressions, and frozen effective frames. Aggregate window
  filters and built-in frame coercion keep their existing rules.
- Named and inherited windows resolve during analysis. Functions reference
  block-owned `WindowId` values; parser window names and inheritance links do
  not enter HIR.
- `RAISE(ABORT, ...)` expressions, including built-in custom-type encoders such
  as `VARCHAR`.
- Custom-type read functions. `union_tag`, `union_extract`, and
  `struct_extract` carry resolved types and stable member indexes in HIR.
- Sequence writes. `nextval` and `setval` carry the resolved sequence,
  backing table, and optional `sqlite_sequence` table. `currval` stays an
  ordinary scalar call because it only reads connection state.
- Basic joins. Comma, plain, inner, and cross joins preserve source order and
  bind `ON` expressions against the complete FROM scope.
- `USING` and `NATURAL` joins resolve both column expressions once, freeze
  comparison rules in HIR, and expose one merged unqualified column while
  keeping qualified columns available.
- Left, right, and full joins remain in lexical source order. Merged columns
  select the left value, right value, or a fact-aware coalesced value according
  to the preserved join kind; no planner-era right-join swap enters HIR.
- Compound SELECT arms become ordered query blocks with separate source scopes,
  aggregate identities, and window identities. `UNION`, `UNION ALL`, `EXCEPT`,
  and `INTERSECT` are preserved in HIR, while outward output identity stays with
  the first arm and width errors keep the existing diagnostic.
- Ordinary CTEs bind lazily from immutable AST references. Referenced CTEs own
  HIR queries and sources, forward sibling references and nested shadowing keep
  their existing rules, and unused invalid definitions never enter the closed
  document. Document-local `CteId` values are allocated on first use so the HIR
  arena contains no unreachable definitions.
- Derived `FROM` subqueries own child queries with lexical parents and become
  `SourceKind::Derived` sources. Derived sources and CTEs share the same
  first-arm output-to-column fact builder. Derived sources remain
  non-correlated at this checkpoint.
- Scalar and `EXISTS` expression subqueries own child queries with lexical
  parents. Outer names resolve through nested HIR scopes, scalar results keep
  the selected output facts, and each query derives its exact sorted capture
  set from completed HIR instead of mutable binding sidecars.
- Built-in array table columns. `SourceColumn::type_fact` carries array rank and
  element type without adding semantic storage metadata beside the column.

The standalone SELECT expression checkpoints are complete except for `IN`
query expressions. `union_value` remains with DML because it needs the
destination column type.

Custom-type table columns, including custom array element programs, remain
separate from built-in array columns.

The supported SELECT path now reaches ordinary non-recursive CTEs and derived
`FROM` sources, plus correlated scalar and `EXISTS` expressions. `IN` query
expressions, `VALUES`, WHERE, GROUP BY, ORDER BY, LIMIT, and recursive CTEs
remain separate checkpoints. Continue with `IN` query expressions before
starting the remaining SELECT clauses.

## Working rules

- Keep existing binding rules and diagnostics.
- Keep parser AST immutable; expression binding returns HIR.
- Successful analysis must produce a closed document that passes
  `HirDocument::validate()`.
- Prefer narrow vertical checkpoints over mechanically translating old binder
  modules.
- Before each code checkpoint, show the proposed shape and obtain approval.
- Test and commit each approved checkpoint separately with `jj`.
- Tests should assert resolved HIR and document validity, not AST mutation or
  `TableReferences` sidecars.
