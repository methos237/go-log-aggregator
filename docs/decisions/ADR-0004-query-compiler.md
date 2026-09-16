# ADR-0004: Query compiler

- **Status:** Accepted
- **Date:** 2026-09-13
- **Supersedes:** none

## Context

The read side, built in phase 4, takes a query in a LogQL-shaped DSL and
answers it from TimescaleDB. It is the only place user-controlled text is
turned into SQL, so its design is dominated by one requirement: no user byte
may ever reach statement text. Secondary requirements are that the hypertable
scan stays cheap, that aggregations over long ranges do not read raw rows, and
that the compiler is unit-testable and fuzzable without a database.

## Decisions

### 1. One pure package, one I/O package

`internal/query` holds lexer, parser, AST and planner together, with the lexer
and tokens unexported. The project plan (not committed) sketched four
packages; one is enough for 1.8k lines and keeps the grammar, its error
positions and the planner's assumptions about the AST in one place. The
package does no I/O: `Compile` returns two statements and their arguments,
nothing more. The executor that hands them to pgx is
`internal/query/executor`, so the compiler can be fuzzed for a minute per
target with no container.

Rejected:

- The four packages (`lexer`, `parser`, `ast`, `planner`) of the plan. At this
  size the package boundaries would have exported the token types and split the
  grammar from the planner's assumptions about it, for no reader's benefit.
- Executing from the same package. A compiler that touches pgx needs a database
  to test, and the fuzz targets would have needed a container.

### 2. Placeholders are minted in one place

A statement is assembled by a `stmt` type whose only way to write a `$n` is
`arg(v)`, which appends `v` and returns the placeholder. Every literal goes
through it, including the *names* of non-promoted labels (`labels ->> $n`)
and the capture index of a `regexp` stage. The identifiers written as text
are the fixed table and column names, and a unit test tokenizes every golden
statement and fails on any word outside that allow-list. An ast-grep rule
forbids a literal `"$1"` anywhere in the compiler so the invariant cannot be
bypassed by hand, and a second rule forbids concatenated or formatted SQL at
any `Query`/`QueryRow`/`Exec` call in the repository.

### 3. Two statements, not a join

The selector is resolved against `streams` first; the resulting ids are bound
into `stream_id = ANY($1)` on the hypertable. The streams table is small and
indexed on `(service, env)` and on `labels` with `jsonb_path_ops`, so
selector work happens there, and the hypertable scan is a plain
(stream_id, time) range. The executor skips the second statement when the
first returns nothing. `labels @> '{"k":"v"}'` is generated for equality on
extra labels because containment is the one form the GIN index serves; other
operators use `coalesce(labels ->> $n, '')` with Prometheus missing-label
semantics and accept a sequential scan of streams.

Rejected:

- One statement joining `logs` to `streams`. The selector predicates would move
  into the join and the hypertable scan would no longer be a plain
  `(stream_id, time)` range; the streams table is small and indexed, so the
  selector work belongs there.

### 4. `level` is a reserved label

`{service="api", level>="warn"}` filters records, not streams: `level` names
the `logs.level` column, its value must be a level name, and the regex
operators are rejected at parse time. Level is the one attribute every query
wants and that varies per record, so a per-stream label would be the wrong
grain.

Rejected:

- `level` as an ordinary stream label. Every level would fingerprint its own
  stream, and a per-record attribute would be filtered at the wrong grain.

### 5. Pipeline semantics

All stages filter records. Line filters test `message`: `|=`/`!=` are `ILIKE`
with LIKE metacharacters escaped; `|~`/`!~` are unanchored regexes, unlike
selector regexes, which are anchored `^(?:...)$`. A label name in a filter or
a `by` clause resolves to the level column, a promoted stream column (joining
`streams`), or otherwise a record field: from the nearest preceding parser
stage, or from the `fields` column the agent populated when there is none.
Extra stream labels are not addressable in the pipeline; they belong in the
selector.

`json` and numeric comparisons cast through `pg_input_is_valid`
(PostgreSQL 16+) so one malformed line yields NULL instead of aborting the
query. `logfmt` is a `substring` over a regex built from the escaped key,
carried as an argument. `regexp` rewrites Go named groups, which PostgreSQL's
ARE dialect lacks, to plain groups and subscripts `regexp_match` by index.

Amended 2026-09-16: this section originally said phase 8 would benchmark
`pg_trgm` and `tsvector` against the `ILIKE` baseline and land the winner
here. Phase 8 measured both (ADR-0008 §6) and changed nothing in the compiler:
`ILIKE` stays, and a trigram index is an opt-in that the existing predicate
uses automatically when present.

Rejected:

- `tsvector` full-text search, measured in ADR-0008 §6. It matches words, not
  substrings, so `|=` would have stopped meaning "contains".
- Shipping a trigram index in a migration. Every deployment would pay its write
  cost for a query shape most never run.

### 6. Source selection is an exactness rule

An aggregation's request range is first widened outward to whole buckets,
counting from `time_bucket`'s origin so the boundaries agree with Postgres's
for every width; the range actually covered is returned as `start` and `end`.
It is then answered from `logs_rate_1m` or `logs_rate_1h` only when the query
has no stages, counts rather than bytes, groups only by level or promoted
labels, and its bucket width is a multiple of the aggregate's grain, so each
aggregate bucket lies entirely inside one output bucket. A partially covered
bucket would be counted whole or not at all, so this is the condition for a
correct answer, not a tuning knob. The coarsest aggregate that fits is used
and the choice is reported as `source`, which the integration test asserts.

Rejected:

- The range-and-bucket heuristic of the plan, which would pick the coarse
  aggregate once the range is long enough. A bucket the aggregate only partly
  covers gives a wrong answer, not a slower one, so the choice has to be a proof
  of exactness rather than a threshold.

### 7. Regex dialects

Go's `regexp` validates patterns at parse time; Postgres's ARE dialect
evaluates them. The parser rejects the Go-only syntax with no ARE spelling
(`\p`, `\Q...\E`, inline flags anywhere but the start) so the mismatch is a
400, and the planner rewrites the escapes that only differ in spelling
(`\b`/`\B` to `\y`/`\Y`, `\z` to `\Z`). Anything else the two dialects
agree on for the patterns a log query realistically uses.

Rejected:

- Letting Postgres be the validator. A pattern Go accepts and ARE does not
  would surface as a failed statement, reported generically as a database
  error, when the fault is in the query.

### 8. Fail-closed API

`/v1/*` requires a static bearer token compared in constant time. An empty
token refuses every request with a message naming the variable to set; there
is no unauthenticated mode. The handler compiles before executing so planner
rejections are 400s and every executor error is database-side, reported
generically and logged in full.

Rejected:

- Serving unauthenticated when no token is configured. A missing variable would
  then be an open API rather than a startup message naming what to set.

## Consequences

- Adding a SQL construct to the planner means extending the allow-list test,
  which is the point: every new identifier is a reviewed decision.
- Grouping by an extra stream label is not possible yet; it needs either a
  syntax to disambiguate stream labels from fields or a decision to shadow
  one with the other.
- Substring search is case-insensitive and unindexed by default. A deployment
  that wants needle-in-haystack lookups adds the opt-in trigram index from
  ADR-0008 §6; the generated SQL is the same either way. (Amended 2026-09-16:
  originally "unindexed until phase 8".)
- The row cap applies to aggregations too; the response says `truncated` so
  a cut series is never mistaken for a complete one.
- Two performance questions are open and were not measured in phase 8, whose
  benchmarks went to the write path: whether a dedicated
  `(stream_id, time DESC, seq DESC)` index on `logs` pays for its write cost,
  and whether extractor expressions should be hoisted into a `LATERAL`
  subquery, since a `json` label used in a numeric filter and a `by` clause is
  parsed once per reference. In the phase 8 runs the scan was served by chunk
  exclusion and the dedup index's `stream_id` prefix. (Amended 2026-09-16:
  originally "left to phase 8's benchmarks".)
