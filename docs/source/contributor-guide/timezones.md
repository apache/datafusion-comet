<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Timezone Handling

This page describes how Comet represents timestamps and applies the Spark session timezone. It is
aimed at contributors working on datetime expressions, casts, scans, or any native code that
produces or consumes timestamp columns. For user-facing differences from Spark, see the
datetime and cast sections of the
[Expression Compatibility](../user-guide/latest/compatibility/expressions/index.md) pages, and the
[scan compatibility](../user-guide/latest/compatibility/scans.md) notes. Known bugs are tracked in
[#6335](https://github.com/apache/datafusion-comet/issues/6335).

The short version: Comet never converts timestamp values at the JVM/native boundary. The session
timezone is not part of a value. It travels with each timezone-aware expression and is applied
inside the kernel that needs it.

## How Spark models time

| Spark type                                     | Stored value                                               | Timezone                                                    |
| ---------------------------------------------- | ---------------------------------------------------------- | ----------------------------------------------------------- |
| `TimestampType` (`TIMESTAMP`, `TIMESTAMP_LTZ`) | Microseconds since the Unix epoch: an instant              | The session timezone, when converting to or from local time |
| `TimestampNTZType` (`TIMESTAMP_NTZ`)           | Microseconds since the epoch of a wall-clock date and time | None                                                        |
| `DateType`                                     | Days since the epoch                                       | None                                                        |

The session timezone is `spark.sql.session.timeZone`. Its default is the JVM's default timezone
when the session is created. On Linux hosts and containers whose `/etc/localtime` points at
`Etc/UTC`, which is the default on Ubuntu and Debian images, that is `Etc/UTC`, not `UTC`.

During analysis, Spark's `ResolveTimeZone` rule stamps the session timezone onto every
`TimeZoneAwareExpression`, and such an expression is not resolved until it has one. `Cast` is the
exception. It needs a timezone only when `Cast.needsTimeZone(from, to)` is true: string to or from
timestamp, timestamp to or from date, timestamp to or from `TIMESTAMP_NTZ`, and those same pairs
nested in arrays, maps and structs. A cast that Spark or Comet creates after analysis can therefore
legitimately arrive with `timeZoneId = None`.

Spark uses the session timezone to:

- convert a `TimestampType` value to or from a string, a date, or a `TimestampNTZType` value
- extract local fields: `hour`, `minute` and `second`, and the date fields through an implicit
  cast to date
- truncate (`date_trunc`), format (`date_format`, `from_unixtime`) and parse (`to_timestamp`,
  `unix_timestamp` of a string or a date)
- evaluate `from_utc_timestamp`, `to_utc_timestamp` and `convert_timezone`
- add calendar intervals, whose day and month units follow local time

It does not use the timezone for comparisons, sorting, hashing, `unix_timestamp` of a timestamp,
or `TimestampNTZType` values, apart from converting them to `TimestampType`. For a
`TimestampNTZType` input, the field extractors use UTC regardless of the session timezone
(`zoneIdForType` in Spark's `datetimeExpressions.scala`).

## How Comet represents timestamps

| Spark type         | Arrow type in native code             |
| ------------------ | ------------------------------------- |
| `TimestampType`    | `Timestamp(Microsecond, Some("UTC"))` |
| `TimestampNTZType` | `Timestamp(Microsecond, None)`        |
| `DateType`         | `Date32`                              |

The `"UTC"` is a label, not a conversion. A `TimestampType` value is already a UTC instant, so
Comet passes the raw microseconds across the boundary in both directions without shifting them.
The label is set in these places:

- `to_arrow_datatype` in `native/core/src/execution/serde.rs`, for every serialized type: scan
  schemas, expression types and literals.
- `CometArrowStream.NATIVE_TIMEZONE` in `CometNativeArrowSource.scala`, which every JVM producer
  uses when it exports Spark data to native code. That includes `CometSparkToColumnarExec`,
  `CometLocalTableScanExec`, the in-memory cache serializer, and the inputs to shuffles and writes.
- The output schema of the codegen dispatcher, in `CometBatchKernelCodegenOutput.scala`.

On the way back, `Utils.fromArrowType` maps any microsecond timestamp that has a timezone to
`TimestampType`, and `CometVector` reads the values unchanged.

### The invariant

Inside a native plan, every `TimestampType` value must carry exactly `"UTC"`, and every
`TimestampNTZType` value no timezone at all. Several things depend on this:

- Arrow's comparison kernels require identical types. `Timestamp(µs, "Etc/UTC")` compared with
  `Timestamp(µs, "UTC")` fails with `Invalid comparison operation`, even though the values are
  comparable. DataFusion's `BinaryExpr` does not coerce, and Comet builds binary expressions
  directly from Spark's already-typed plan.
- Native datetime kernels decide between wall-clock and instant semantics from the label. A
  `TimestampType` value labelled `None` is treated as `TimestampNTZType` and silently loses the
  session timezone.

A mislabelled column is easy to miss in tests. `ScanExec` casts every column it imports from the
JVM to its declared type, and the shuffle writer's `SchemaAlignExec` does the same before it
partitions, so a wrong label disappears at the next stage boundary. `CASE`, `COALESCE` and `IF`
cast a branch whose Arrow type differs from the others to a common type (`coerce_branch` in
`native/spark-expr/src/conditional_funcs/case_when.rs`). For a timestamp that cast only changes the
label, so a mislabelled branch does not fail there either. A test that only projects the result
passes. The label only matters when the result is compared or feeds another native expression.

## How the session timezone reaches native code

Each timezone-aware serde reads the timezone that Spark stamped on the expression
(`expr.timeZoneId`), converts it with `CometTimeZone.nativeId`, and serializes the result into the
expression's protobuf message. That includes `Cast`, `Hour`, `Minute`, `Second`, `UnixTimestamp`,
`TruncTimestamp`, `ToJson`, `ToCsv` and `ToPrettyString`. Serdes should use this value rather than
`SQLConf.get.sessionLocalTimeZone` or the JVM default, because it is what Spark itself evaluates
the expression with.

When the expression has no timezone, `nativeId` returns `"UTC"`. Spark does not resolve a
timezone-aware expression without one, so this only happens for casts that do not use the
timezone. Native code reports an empty timezone as an error (`require_timezone` in
`native/spark-expr/src/utils.rs`) rather than guessing one.

On the native side, `array_with_timezone` in `native/spark-expr/src/utils.rs` is the common entry
point:

- A `TimestampType` input is relabelled with the session timezone. For casts to strings and dates,
  the values are also shifted to local time and the label is dropped.
- A `TimestampNTZType` input is left alone, except when it is cast to `TimestampType`. In that case
  the wall-clock value is resolved in the session timezone.

DST transitions follow Java (`resolve_local_datetime`). An ambiguous local time takes the earlier
offset. A local time inside a gap resolves with the offset that applied before the transition,
which gives the same instant as Java's `atZone`.

A native kernel that produces a `TimestampType` value must do its local-time work in the session
timezone and then return the result labelled `"UTC"`. That applies both to the declared type, in
`data_type()` or `return_type()`, and to the arrays it builds.

DataFusion's own datetime functions take their timezone from the argument's label, or from
`datafusion.execution.time_zone`, which Comet leaves unset. Wiring one of them in for a
timezone-aware Spark expression therefore evaluates it in UTC, unless the session timezone is
passed explicitly. For example, `to_char` formats in the input's label, which is why `date_format`
runs natively only in UTC sessions.

### Parsing timezone IDs

Spark resolves session timezone IDs with `ZoneId.of(id, ZoneId.SHORT_IDS)`, which accepts forms
such as `Z`, offsets like `+8` and `+08:00:00`, prefixed offsets like `GMT+8`, and short IDs like
`PST`. Native code parses timezone IDs with arrow's `Tz::from_str`, which accepts only IANA names
such as `America/Los_Angeles` and fixed offsets written as `+HH`, `+HHMM` or `+HH:MM`.

`CometTimeZone.nativeId` in `spark/src/main/scala/org/apache/comet/serde/CometTimeZone.scala`
bridges the two. It normalizes the zone first, so an ID whose offset never changes becomes `UTC`
for a zero offset, which covers `Z`, `GMT` and `Etc/UTC`, or `+HH:MM` otherwise. A short ID becomes
its region. An offset with seconds, such as `+05:45:30`, has no native spelling, so `nativeId`
returns `None`. The serde then reports the expression as unsupported through
`CometTimeZone.supportLevel`, and it runs in the codegen dispatcher or falls back to Spark.

Code that takes a fast path for UTC should still compare the ID against a fixed list of UTC
aliases, and send everything else down the general path. The list in `extract_date_part.rs` is an
example.

### Timezone rules

Spark converts between instants and local time using the JVM's timezone rules (`tzdb.dat`). Native
code uses chrono-tz, which compiles its own copy of the IANA database into libcomet
(`chrono_tz::IANA_TZDB_VERSION`), and its precomputed DST transitions end around 2100. The two can
disagree, both for zones whose rules changed between the two database versions and for far-future
timestamps. When the library loads, `NativeBase` compares the native version (`getTzdataVersion`)
with the JVM's and logs a warning if they differ. The user guide describes the effect under
"Timezone Database Versions" on the datetime
[expression compatibility](../user-guide/latest/compatibility/expressions/index.md) page.

## Scans

Parquet stores timestamps either as `INT64` annotated `TIMESTAMP(MICROS or MILLIS, isAdjustedToUTC)`,
or as legacy `INT96`. Comet's native scan follows Spark's vectorized reader, which never shifts a
value by a timezone:

| Parquet column                       | Spark type         | Comet                                                                                         |
| ------------------------------------ | ------------------ | --------------------------------------------------------------------------------------------- |
| `TIMESTAMP`, `isAdjustedToUTC=true`  | `TimestampType`    | Read as is. Milliseconds are scaled to microseconds.                                          |
| `TIMESTAMP`, `isAdjustedToUTC=false` | `TimestampType`    | Relabelled without shifting, as Spark does                                                    |
| `TIMESTAMP`, `isAdjustedToUTC=false` | `TimestampNTZType` | Read as is                                                                                    |
| `TIMESTAMP`, `isAdjustedToUTC=true`  | `TimestampNTZType` | Rejected on Spark 3.x (SPARK-36182). Relabelled without shifting on Spark 4.0+ (SPARK-47447). |
| `INT96`                              | `TimestampType`    | Coerced to microseconds and labelled `"UTC"`                                                  |

The `INT96` coercion is configured by `coerce_int96` and `coerce_int96_tz` in `parquet_exec.rs`. An
`INT96` column read as `TimestampNTZType` follows the `isAdjustedToUTC=true` row.

The timestamp-to-timestamp adaptations go through `CometCastColumnExpr` and
`parquet_convert_array` in `native/core/src/parquet/`, not through Spark's `Cast`. A Spark cast
between `TimestampType` and `TimestampNTZType` would apply the session timezone, and the Parquet
reader does not.

Two Spark settings are not handled natively:

- Comet disables itself for a session with `spark.sql.parquet.int96TimestampConversion=true`,
  which shifts `INT96` values written by Impala.
- Comet does not rebase dates and timestamps that were written with the legacy hybrid calendar.
  See [#5010](https://github.com/apache/datafusion-comet/issues/5010). Spark's rebase of legacy
  timestamps is itself timezone-dependent.

For Iceberg, iceberg-rust labels `timestamptz` columns `Timestamp(Microsecond, "+00:00")`. The
Iceberg scan adapts its batches to the Spark schema, which relabels them `"UTC"`. Iceberg's
partition transforms (`years`, `months`, `days` and `hours`) are defined in UTC.

The native CSV scan, which is only enabled for testing, parses a timestamp without an offset as
UTC. Spark parses it in the CSV `timeZone` option, which defaults to the session timezone, so
`CometScanRule` falls back when the read schema has a `TimestampType` column and that timezone is
not UTC.

## The codegen dispatcher

Several timezone-dependent expressions have no native implementation that is compatible in every
session, including `date_trunc`, `date_format`, `from_unixtime`, `from_utc_timestamp`,
`to_utc_timestamp`, `make_timestamp` and `to_timestamp`. Their serdes route the cases the native
path cannot handle through the JVM codegen dispatcher, as they do for a session timezone that
`CometTimeZone.nativeId` cannot express. The dispatcher runs Spark's generated code with the
`timeZoneId` stamped on the expression, so the results match Spark, and its Arrow output is
labelled `"UTC"` like everything else.

## Guidelines

- Never read `TimeZone.getDefault`, `ZoneId.systemDefault` or the host's local time in Comet code.
  Spark's semantics come from the timezone stamped on the expression. The JVM default only matters
  as the default value of `spark.sql.session.timeZone`.
- Pass a timezone to native code only through `CometTimeZone.nativeId`, and return
  `CometTimeZone.supportLevel` from `getSupportLevel` when it gives `None`.
- Never label a `TimestampType` value with the session timezone, and never return
  `Timestamp(_, None)` for a `TimestampType` result.
- Don't assume `"UTC"` is the only UTC session timezone. `Etc/UTC` is the common default, and a
  native path gated on "the session is UTC" must still return `"UTC"`-labelled output there.
- Don't apply a timezone to a `TimestampNTZType` value, except when converting it to
  `TimestampType`.

## Testing timezone-sensitive code

- Run in several session timezones: `UTC`, `Etc/UTC`, a zone with DST such as
  `America/Los_Angeles`, and a zone with a half-hour offset such as `Asia/Kolkata`. SQL file tests
  can use `-- ConfigMatrix: spark.sql.session.timeZone=...` (see
  [Comet SQL Tests](sql-file-tests.md)). `session_timezone_ids.sql` runs the forms that
  `CometTimeZone.nativeId` rewrites, such as `GMT+8`, `Z` and `PST`.
- Use the result, rather than only projecting it. Compare it with another timestamp, and feed it to
  `hour` or a cast to string. A wrong label only shows up there.
- Include timestamps around DST transitions, before the epoch, and before 1900, when many zones
  used local mean time offsets.
- Avoid dates in zones whose rules changed in recent tzdata releases unless the test needs them. A
  JDK with older timezone data gives Spark different answers there, and the warning from
  `NativeBase` shows when the two versions differ.
- Build inputs from Parquet tables rather than `VALUES` lists. The optimizer evaluates a
  projection over `VALUES` itself, so Comet never runs the expression.
- When collecting timestamps in Scala tests, set `spark.sql.datetime.java8API.enabled=true`, or
  cast to strings. `java.sql.Timestamp` conversion goes through the JVM's default timezone and the
  hybrid calendar.
- Check that the expression ran natively, not only that the plan has Comet operators. The codegen
  dispatcher returns Spark's answer from inside a Comet operator, and a dispatched expression takes
  its whole subtree with it, including children that have a native implementation. `expect_native`
  in SQL file tests and `checkSparkAnswerAndImpl` in Scala tests check which one ran.
