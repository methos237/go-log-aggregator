# Query language reference

A LogQL-shaped language compiled to parameterized TimescaleDB SQL. A query names a
set of streams, optionally filters and reshapes their records, and optionally
aggregates them into a time series. This document is the complete surface; the
package documentation of [`internal/query`](../internal/query/ast.go) carries the
same grammar, and a test fails if the two drift apart. The reasoning behind the
design is in [ADR-0004](decisions/ADR-0004-query-compiler.md).

```
{service="api", env!="dev"} |= "timeout" | json | status >= 500 | rate(5m) by (level)
└──────── selector ────────┘ └ line ─┘ └parser┘ └ label filter ┘ └── aggregation ──┘
```

## Grammar

```ebnf
query        = selector { line_filter | "|" stage } [ "|" aggregation ] ;
selector     = "{" matcher { "," matcher } "}" ;
matcher      = label op string ;
op           = "=" | "!=" | "=~" | "!~" | ">=" | "<=" | ">" | "<" ;
line_filter  = ( "|=" | "!=" | "|~" | "!~" ) string ;
stage        = parser_stage | label_filter ;
parser_stage = "json" | "logfmt" | "regexp" string ;
label_filter = label op ( string | number ) ;
aggregation  = func "(" duration ")" [ "by" "(" label { "," label } ")" ] ;
func         = "rate" | "count_over_time" | "bytes_over_time" ;
duration     = number ( "s" | "m" | "h" | "d" ) ;
string       = Go string literal, "double-quoted" or `raw` ;
```

Line filters attach directly to the selector with no leading `|`, as in LogQL.
Parser stages, label filters and the aggregation are each introduced by `|`.
Stages run in source order, and the aggregation must be last. Whitespace between
tokens is free. Strings follow Go syntax: `"..."` with backslash escapes, or
`` `...` `` raw, which is the easier form for a regex.

Errors carry a `line:col` position and the token they stopped at:

```
$ logctl query '{service="api"} | json | status >= '
1:36: expected string, got end of input
```

## Selector

The selector is required and has at least one matcher. Each matcher compares one
stream label to a string. Streams are the deduplicated label sets the agent sent;
`service`, `host` and `env` are *promoted* labels with their own indexed columns,
and every other label lives in a JSONB document.

| Operator | Meaning | Note |
|---|---|---|
| `=` | equal | For a non-promoted label this is JSONB containment, so the label must be present: `{region=""}` does not match a stream that has no `region`. |
| `!=` | not equal | A missing label compares as `""`, so `{region!="eu"}` matches streams with no `region`, as in Prometheus. |
| `=~` | matches regex | Anchored: the pattern must match the whole value. `{service=~"api"}` does not match `api-gateway`; write `{service=~"api.*"}`. |
| `!~` | does not match regex | Anchored likewise. |
| `>=` `<=` `>` `<` | ordering | Bytewise on the text (`COLLATE "C"`), so `"B" < "a"`. Mostly useful with `level`. |

```
{service="api"}
{service=~"api-.*", env!="dev"}
{host="web-03", region="eu"}
```

The planner resolves the selector against the `streams` table first and then scans
the hypertable for `stream_id = ANY($1)`. A selector that matches no stream returns
an empty result without touching the log rows.

### The reserved label `level`

`level` is not a stream label. It names the record's severity column, so
`{service="api", level>="warn"}` matches the `api` streams and then keeps only
records at warn or above. Levels are ordered `trace < debug < info < warn < error <
fatal`, and `unspecified` sits below `trace` for records whose source had no level.
Every operator except the regex pair is allowed, and the value must be a level name
or one of its aliases (`warning`, `err`, `critical`, `panic` and so on; the full
list is in [`internal/model/level.go`](../internal/model/level.go)). Unknown names
are a parse error.

```
{level>="warn"}                      every stream, warnings and worse
{service="api", level="error"}
{service="api"} | level != "debug"   the same test as a pipeline stage
```

## Line filters

A line filter tests the raw message text. Several may follow the selector; all must
pass.

| Operator | Meaning | Note |
|---|---|---|
| `\|= "text"` | contains | Case-insensitive substring search (`ILIKE`). `%` and `_` in the text are matched literally. |
| `!= "text"` | does not contain | Same search, negated. |
| `\|~ "re"` | matches regex | **Unanchored**: a search, not a whole-line match. `.` matches a newline. |
| `!~ "re"` | does not match regex | Same, negated. |

```
{service="api"} |= "timeout"
{service="api"} |= "GET" != "/healthz"
{service="api"} |~ `status=(5\d\d)`
```

Substring search is a sequential scan over the matching rows. On a large range,
narrow the selector or the time window first. A `pg_trgm` index makes `|=` roughly
fifty times faster on a needle-in-haystack search and is used automatically when
present; it is opt-in because it costs write throughput
([ADR-0008 §6](decisions/ADR-0008-benchmark-driven-defaults.md)).

## Parser stages

A parser stage extracts labels from the message so that later label filters and
`by` clauses can use them. It does not filter by itself, and it does not change
what a log query returns; records come back with their original message.

| Stage | Extracts | Missing value |
|---|---|---|
| `\| json` | Top-level keys of the message parsed as a JSON object. Nested values are not reachable. | A message that is not a JSON object yields no labels; every label filter after it that needs one fails for that record. |
| `\| logfmt` | The first `key=value` or `key="quoted value"` pair for the requested key. Quotes are stripped. | A key that does not appear yields no label. |
| `\| regexp "pattern"` | Named capture groups, `(?P<name>...)` or `(?<name>...)`, matched anywhere in the message. | A label that is not a group name is rejected before the query runs; an unmatched pattern yields no labels. |

Before any parser stage, label filters read the structured fields the agent
extracted at ingest, which for a JSON log line are already its top-level keys. A
later parser stage replaces the earlier one: after `| json | logfmt`, `status`
comes from logfmt.

```
{service="api"} | json | status >= 500
{service="api"} | logfmt | route = "/v1/query"
{service="nginx"} | regexp `" (?P<status>\d{3}) (?P<bytes>\d+)$` | status = "404"
```

## Label filters

A label filter compares an extracted label, a promoted stream label or `level`.
The right-hand side is a string or a bare number, and which one changes the
comparison.

| Right-hand side | Comparison |
|---|---|
| `"string"` | Text. `=~` and `!~` are anchored regexes as in the selector; ordering is bytewise. A missing label compares as `""`. |
| `number` (`500`, `4.5`, `-1`) | Numeric. The label's text is cast to `numeric`, so `"4.50"` equals `4.5`. A missing or non-numeric value never matches, for any operator including `!=`. |

```
{service="api"} | json | status >= 500
{service="api"} | json | status = "500"          text: "500" only, not "500.0"
{service="api"} | json | user_agent =~ "curl.*"
{service="api"} | host != "canary-01"            promoted label, no parser needed
{service="api"} | duration_ms > 250              agent-extracted field
```

Only `level`, the promoted labels `service`, `host` and `env`, and extracted labels
may appear in a label filter or a `by` clause. Other stream labels belong in the
selector.

## Aggregations

An aggregation ends the query and turns records into one row per time bucket and
`by` group. The result is `points` rather than `records`.

| Function | Value per bucket |
|---|---|
| `rate(d)` | records per second: count divided by the bucket width in seconds |
| `count_over_time(d)` | record count |
| `bytes_over_time(d)` | sum of message length in bytes |

`d` is a positive integer with a unit: `30s`, `5m`, `1h`, `7d`. The request's
`start` and `end` are widened outward to whole buckets, aligned to the same origin
TimescaleDB's `time_bucket` uses, and the widened range is echoed in the response.
`by` takes `level`, the promoted labels, or extracted labels. Groups sort in byte
order so that every node in a cluster merges shards identically.

```
{service="api"} | rate(1m)
{service="api", level>="warn"} | count_over_time(5m) by (level)
{env="prod"} | bytes_over_time(1h) by (service, host)
{service="api"} | json | count_over_time(1m) by (status)
```

### Which relation answers

The planner reads a continuous aggregate instead of the hypertable when it can
prove the answer is identical: the query has no pipeline stages, the function is
`rate` or `count_over_time`, every `by` label is `level` or a promoted label, and
the bucket width is a whole multiple of the aggregate's. `logs_rate_1m` serves
widths that are whole minutes and `logs_rate_1h` whole hours; the coarsest that
fits wins. The response reports the choice as `source`, one of `logs`,
`logs_rate_1m` or `logs_rate_1h`. `bytes_over_time` and any query with a stage
always read `logs`.

| Query | `source` |
|---|---|
| `{service="api"} \| rate(5m) by (level)` | `logs_rate_1m` |
| `{service="api"} \| count_over_time(2h)` | `logs_rate_1h` |
| `{service="api"} \| rate(90s)` | `logs` (not a whole minute) |
| `{service="api"} \|= "GET" \| rate(5m)` | `logs` (a line filter is a stage) |
| `{service="api"} \| json \| rate(5m) by (status)` | `logs` |

## Regular expressions

Patterns are validated with Go's `regexp` at parse time and evaluated by
PostgreSQL's ARE engine at query time. The parser rejects the Go-only syntax ARE
cannot evaluate, so a query that parses will run:

- `\p{...}` and `\P{...}` Unicode classes, and `\Q...\E` quoting, are rejected.
- Inline flags such as `(?i)` are allowed only at the very start of the pattern.
- `\b`, `\B` and `\z` mean what they mean in Go; the planner respells them for ARE.
- Named groups are `(?P<name>...)` or `(?<name>...)`.
- `.` matches a newline in both engines.
- A pattern is at most 1 024 bytes.

Selector and label-filter regexes are anchored to the whole value. Line-filter
regexes are not.

## Limits and results

A log query always carries a time range and a `LIMIT`; nothing can ask the
hypertable for everything. Over HTTP, `start` and `end` default to the last hour,
`limit` defaults to 1 000 and is capped by `LOGAGG_HTTP_QUERY_MAX_ROWS`, and
`direction` is `backward` (newest first, the default) or `forward`. `truncated` is
true when more rows matched than the limit allowed. `LOGAGG_HTTP_QUERY_TIMEOUT`
bounds every request. In a cluster, `warnings` lists any member whose share of the
streams could not be searched; the rows from the others are still returned.

`logctl query` takes the same parameters as flags. `logctl tail` and `GET
/v1/tail` accept the selector, including `level`, and line filters only; parser
stages, label filters and aggregations are rejected with the position of the
offending stage, because a tail is evaluated in memory on records as they arrive
rather than by the database.

## Safety

No byte of a query ever reaches SQL text. Every literal, including the *name* of a
non-promoted label, becomes a `$n` argument, and the only identifiers written into
a statement are fixed table and column names. Tests assert the generated SQL
contains no bytes from the input, an `ast-grep` rule (`make lint-arch`) forbids
concatenated SQL at any query call and any literal `$n` outside the one function
that mints placeholders, and `make fuzz` runs the lexer, parser and planner. A
collector that receives a compiled statement from a peer runs `query.CheckSQL` on
it before execution, so the peer port is not a way to run arbitrary SQL either.

## Worked examples

**Errors from one service in the last fifteen minutes**

```
logctl query -since 15m '{service="api", level>="error"}'
```

**Slow requests, from a JSON log**

```
logctl query '{service="api"} | json | route = "/v1/query" | duration_ms > 500'
```

**Which status codes a service returned, per minute**

```
logctl query -since 1h '{service="api"} | json | count_over_time(1m) by (status)'
```

**Error rate per service across production, from the hourly aggregate**

```
logctl query -since 7d '{env="prod", level>="error"} | rate(1h) by (service)'
```

**Everything mentioning a request id, from any service**

```
logctl query '{env="prod"} |= "req-7f3a9c"'
```

**Watch a deploy**

```
logctl tail '{service="api", host=~"canary-.*", level>="warn"}'
```
