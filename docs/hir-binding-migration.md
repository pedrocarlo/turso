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

SELECT binding is complete. DML binding is in progress.

Completed:

- Analyzer-owned HIR arenas and mandatory document validation.
- HIR scope with source, output-alias, database-qualified, outer-scope, rowid,
  USING/NATURAL merged-column, star-expansion, and collation rules.
- One ordinary table source and basic result outputs, including stars.
- Immutable AST-to-HIR binding for names, literals, unary and binary operators,
  null tests, parentheses, `COLLATE`, and SQL parameters.
- `LIKE`, `GLOB`, `REGEXP`, and `MATCH` expressions carry resolved function
  identity and argument count. Only `LIKE` accepts `ESCAPE`; row-valued left
  sides remain limited to `MATCH`.
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
- Direct and nested struct/union dot access resolves database and table names
  first, then freezes member kind, index, container type, and result type in
  HIR. Function and dot syntax share one member resolver.
- Sequence writes. `nextval` and `setval` carry the resolved sequence,
  backing table, and optional `sqlite_sequence` table. `currval` stays an
  ordinary scalar call because it only reads connection state.
- Basic joins. Comma, plain, inner, and cross joins preserve source order and
  bind `ON` expressions against the complete FROM scope.
- Table-valued functions become `SourceKind::TableFunction` sources with a
  resolved catalog table and bound HIR arguments. Arguments use the complete
  FROM scope, including forward references, while aggregate/window calls and
  excess hidden-column arguments keep their binding-time errors.
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
- Scalar and row-value `IN` query expressions keep the left expressions,
  negation, child query, and one frozen comparison rule per column in HIR.
  Correlated right sides capture outer sources, and row-width errors keep the
  existing diagnostic.
- `IN table` and `IN table(arguments)` normalize to the same membership-query
  HIR. The generated query selects visible source columns from a resolved
  table, CTE, or table function; arguments and CTE bodies keep exact captures.
- WHERE expressions become query-block filters. Source columns win over result
  aliases, aliases remain stable `OutputId` references, correlated subqueries
  keep their captures, and aggregate or window calls retain their existing
  errors.
- GROUP BY keys keep resolved expressions, type facts, and collations. Source
  columns win over aliases, positive integer terms become `OutputId` references,
  and grouping expressions cannot capture an enclosing query. HAVING prefers
  aliases, allows aggregates with block-local identities, and rejects windows.
- Query-level ORDER BY terms keep resolved expressions, direction, null order,
  type facts, and collations. Ordinary queries prefer output aliases, preserve
  output ordinals, correlated subqueries, and ORDER-BY-only function identities.
  Compound queries resolve ordinals and names from any arm to the first arm's
  stable `OutputId` values and keep their restricted-expression diagnostics.
- Query-level LIMIT and OFFSET expressions bind in an empty scalar scope. They
  keep parameters, scalar functions, DQS literals, and independent child
  queries while rejecting query names, correlation, aggregates, and windows.
  Both OFFSET spellings share the same normalized HIR shape.
- VALUES rows bind as block-owned HIR expressions. Generated `columnN` outputs
  keep first-row type, affinity, and collation facts; row expressions remain
  the only runtime values. VALUES subqueries keep outer captures, scalar DQS
  rules, aggregate/window identities, and compound-arm ordering.
- Recursive CTE seeds and recursive arms bind as separate HIR queries.
  Self-references become occurrence-local `RecursiveInput` sources, UNION
  operators and comparison collations stay explicit, and recursive
  aggregate/window and structure errors remain binding-time errors. Queue
  ORDER BY terms resolve to output positions with explicit collations, while
  LIMIT/OFFSET expressions use the same empty scalar scope as ordinary limits.
  Recursive-reference counting follows nested CTE scopes: shadowing definitions
  hide outer names, unused definitions contribute nothing, and referenced
  definitions contribute their body's recursive references. CTE queries nested
  in correlated subqueries inherit the enclosing query scope; recursive seeds
  and arms record exact outer-source captures while same-query sibling sources
  remain invisible.
- Built-in array table columns. `SourceColumn::type_fact` carries array rank and
  element type without adding semantic storage metadata beside the column.
- `ARRAY[...]`, `array(...)`, bracket subscripts, and `array_element(...)`
  normalize to dedicated array/subscript HIR. Constructors derive rank from
  their elements; typed column subscripts retain element type facts.
- Array utility calls keep resolved function identity and derive exact result
  categories. Mutators preserve or deepen array facts, concatenation merges
  ranks, shape-preserving calls retain declarations, and scalar utilities have
  fixed integer or text results.
- Custom binary operators freeze their two-argument function identity,
  direct-or-derived swap/negate behavior, and any literal encoder call. Same
  declared custom types and compatible literals use the operator; different
  types and incompatible literals keep normal SQL behavior.
- `INDEXED BY` resolves to a snapshot-bound index identity on the source, while
  `NOT INDEXED` remains an explicit source hint. Missing and wrong-table indexes
  fail during analysis.
- Custom-type table columns keep their resolved type chain and declaration
  parameters in HIR. Encode programs follow leaf-to-base order, decode programs
  reverse it, and custom arrays reuse element `TypeFact` without separate array
  storage metadata.
- Referenced virtual generated columns and short-record defaults become
  source-owned HIR expressions. A final required-column worklist follows
  generated-column dependencies transitively, while unused stored expressions
  remain `NotRequired`.
- Basic `INSERT ... VALUES` and `DEFAULT VALUES` statements become INSERT HIR.
  Target columns, duplicate-column write selection, rowid aliases, defaults,
  generated expressions, and row-width diagnostics are resolved during
  analysis.
- INSERT targets freeze every CHECK expression plus every ordinary,
  expression, and partial index in catalog order. Stable catalog identities
  and `IndexCoverage::Complete` make the required write metadata explicit.
  Trigger, foreign-key, and AUTOINCREMENT targets remain later checkpoints.
- Plain `INSERT ... SELECT` sources retain their query identity and complete
  SELECT HIR. Destination width is checked during analysis, and nested query
  captures remain attached to the source query tree.
- Top-level INSERT WITH clauses use the same lazy CTE scopes as SELECT.
  Referenced ordinary and recursive CTEs enter the closed document, while
  unused invalid definitions remain unbound.
- Statement-level INSERT conflict resolution remains an exact parser enum in
  HIR, distinguishing ROLLBACK, ABORT, FAIL, IGNORE, REPLACE, and no override.
- Direct INSERT values and defaults carry their destination type through the
  iterative expression frames. `union_value` freezes the destination union and
  tag in HIR, and its value argument receives the selected variant type,
  including for nested unions.
- INSERT-SELECT passes destination types by output position into every SELECT
  and VALUES compound arm. Destination-aware functions therefore resolve the
  same way in direct VALUES and query sources without changing standalone
  SELECT rules.
- Scalar INSERT RETURNING expressions bind against the target source and become
  root-owned HIR outputs. Unqualified and qualified stars, aliases, type facts,
  affinities, collations, and generated-column dependencies are resolved during
  analysis. RETURNING subqueries remain a later root-expression checkpoint.

The standalone SELECT expression checkpoints are complete. `union_value` is
resolved only in destination-aware DML expressions.

The supported SELECT path now reaches ordinary non-recursive CTEs and derived
`FROM` sources, plus correlated scalar, `EXISTS`, and `IN` query expressions.
WHERE filters, GROUP BY keys, HAVING predicates, query-level ORDER BY terms, and
LIMIT/OFFSET and VALUES rows are also bound. Recursive CTE core identity, arms,
queue ORDER BY, LIMIT/OFFSET, and nested CTE identity are bound. Correlation
with enclosing queries is also bound for ordinary and recursive CTE queries.

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
