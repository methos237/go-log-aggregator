# ADR-0004: Query compiler

**Status:** accepted (phase 4)

## Context

The read side takes a query in a LogQL-shaped DSL and answers it from
TimescaleDB. It is the only place user-controlled text is turned into SQL, so
its design is dominated by one requirement: no user byte may ever reach
statement text. Secondary requirements are that the hypertable scan stays
cheap, that aggregations over long ranges do not read raw rows, and that the
compiler is unit-testable and fuzzable without a database.

## Decisions

### One pure package, one I/O package

`internal/query` holds lexer, parser, AST and planner together, with the
lexer and tokens unexported. The roadmap sketched four packages; one is
enough for 1.8k lines and keeps the grammar, its error positions and the
planner's assumptions about the AST in one place. The package does no I/O:
`Compile` returns two statements and their arguments, nothing more. The
executor that hands them to pgx is `internal/query/executor`, so the compiler
can be fuzzed for a minute per target with no container.

### Placeholders are minted in one place

A statement is assembled by a `stmt` type whose only way to write a `$n` is
`arg(v)`, which appends `v` and returns the placeholder. Every literal goes
through it, including the *names* of non-promoted labels (`labels ->> $n`)
and the capture index of a `regexp` stage. The identifiers written as text
are the fixed table and column names, and a unit test tokenizes every golden
statement and fails on any word outside that allow-list. An ast-grep rule
forbids a literal `"$1"` anywhere in the compiler so the invariant cannot be
bypassed by hand, and a second rule forbids concatenated or formatted SQL at
any `Query`/`QueryRow`/`Exec` call in the repository.

### Two statements, not a join

The selector is resolved against `streams` first; the resulting ids are bound
into `stream_id = ANY($1)` on the hypertable. The streams table is small and
indexed on `(service, env)` and on `labels` with `jsonb_path_ops`, so
selector work happens there, and the hypertable scan is a plain
(stream_id, time) range. The executor skips the second statement when the
first returns nothing. `labels @> '{"k":"v"}'` is generated for equality on
extra labels because containment is the one form the GIN index serves; other
operators use `coalesce(labels ->> $n, '')` with Prometheus missing-label
semantics and accept a sequential scan of streams.

### `level` is a reserved label

`{service="api", level>="warn"}` filters records, not streams: `level` names
the `logs.level` column, its value must be a level name, and the regex
operators are rejected at parse time. Level is the one attribute every query
wants and that varies per record, so a per-stream label would be the wrong
grain.

### Pipeline semantics

All stages filter records. Line filters test `message`: `|=`/`!=` are the
`ILIKE` baseline from the roadmap (phase 8 benchmarks `pg_trgm` and
`tsvector` and lands the winner here) with LIKE metacharacters escaped;
`|~`/`!~` are unanchored regexes, unlike selector regexes, which are anchored
`^(?:...)$`. A label name in a filter or a `by` clause resolves to the level
column, a promoted stream column (joining `streams`), or otherwise a record
field: from the nearest preceding parser stage, or from the `fields` column
the agent populated when there is none. Extra stream labels are not
addressable in the pipeline; they belong in the selector.

`json` and numeric comparisons cast through `pg_input_is_valid`
(PostgreSQL 16+) so one malformed line yields NULL instead of aborting the
query. `logfmt` is a `substring` over a regex built from the escaped key,
carried as an argument. `regexp` rewrites Go named groups, which PostgreSQL's
ARE dialect lacks, to plain groups and subscripts `regexp_match` by index. Go
validates patterns at parse time and PostgreSQL evaluates them; the dialects
agree on everything a log query realistically uses and the divergence on
exotic syntax is accepted.

### Source selection is an exactness rule

An aggregation is answered from `logs_rate_1m` or `logs_rate_1h` only when the
query has no stages, counts rather than bytes, groups only by level or
promoted labels, and its bucket width is a multiple of the aggregate's grain
with the request edges on that grain. A partially covered bucket would be
counted whole or not at all, so alignment is the condition for a correct
answer, not a tuning knob. The coarsest aggregate that fits is used and the
choice is reported as `source`, which the integration test asserts.

### Fail-closed API

`/v1/*` requires a static bearer token compared in constant time. An empty
token refuses every request with a message naming the variable to set; there
is no unauthenticated mode. The handler compiles before executing so planner
rejections are 400s and every executor error is database-side, reported
generically and logged in full.

## Consequences

- Adding a SQL construct to the planner means extending the allow-list test,
  which is the point: every new identifier is a reviewed decision.
- Grouping by an extra stream label is not possible yet; it needs either a
  syntax to disambiguate stream labels from fields or a decision to shadow
  one with the other.
- Substring search is case-insensitive and unindexed until phase 8.
