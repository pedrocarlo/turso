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

Remaining expression checkpoints, in agreed order:

1. Finish resolved custom `CAST` programs: `RAISE` in encoders and arrays.
2. Finish function forms: aggregates, windows, argument ordering, filters, and
   special custom-type or sequence calls.

After expression work, resume the original SELECT order at joins. Basic outputs
were implemented early; join-aware output behavior must still be checked after
joins. Clause work such as WHERE must not jump ahead of this sequence without
an explicit plan change.

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
