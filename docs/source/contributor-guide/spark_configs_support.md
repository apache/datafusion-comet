<!---
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

# Supported Spark Configurations

This document tracks Spark SQL configurations that affect Comet's behavior. For each
configuration we record which Comet expressions or operators are influenced, what
verification has been performed, and any known gaps.

## How to Read This Document

The status column uses these values:

- **Supported** -- Comet runs the affected expressions natively under every value of
  the config, and produces results matching Spark.
- **Partial** -- Comet runs natively for some values of the config but falls back to
  Spark for others, or runs natively but with documented incompatibilities.
- **Falls back** -- Comet does not run the affected expressions natively under this
  config and always defers to Spark.
- **Unaudited** -- the config's interaction with Comet has not yet been verified.

## Audited Configurations

- `spark.sql.legacy.timeParserPolicy`
  - Default: `EXCEPTION`
  - Status: Falls back (see notes)
  - Affected expressions: `date_format`, `from_unixtime`, `unix_timestamp`, `to_unix_timestamp`, `to_timestamp`, `to_timestamp_ntz`, `to_date`, `try_to_timestamp` (Spark 4+)
  - Spark versions checked: 3.4.3, 3.5.8, 4.0.1
  - Date: 2026-05-02

## Audit Notes

### `spark.sql.legacy.timeParserPolicy`

**Source.** `SQLConf.LEGACY_TIME_PARSER_POLICY` selects the formatter used by
`TimestampFormatter` and `DateFormatter`:

- `LEGACY` -- `java.text.SimpleDateFormat` / `FastDateFormat`. Lenient parsing.
- `CORRECTED` -- `java.time.DateTimeFormatter` via `Iso8601TimestampFormatter`. Strict.
- `EXCEPTION` (default) -- same parser as `CORRECTED`, plus
  `DateTimeFormatterHelper.checkParsedDiff` raises `SparkUpgradeException`
  (`INCONSISTENT_BEHAVIOR_CROSS_VERSION`) when the new parser fails on input that the
  legacy parser would have accepted. Pattern validation also raises
  `SparkUpgradeException` when a pattern is recognized only by the legacy formatter
  (this check applies under both `CORRECTED` and `EXCEPTION`).

**Affected expressions.** Determined by tracing `TimestampFormatterHelper`,
`TimestampFormatter(...)`, and `DateFormatter(...)` usage in
`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/datetimeExpressions.scala`
across Spark 3.4, 3.5, 4.0, and 4.1. Three expression classes mix in
`TimestampFormatterHelper`:

- `DateFormatClass` -- `date_format`
- `FromUnixTime` -- `from_unixtime`
- `ToTimestamp` (abstract) -- `UnixTimestamp` (`unix_timestamp`),
  `ToUnixTimestamp` (`to_unix_timestamp`), `GetTimestamp` (used by
  `ParseToTimestamp` for `to_timestamp` / `to_timestamp_ntz`, `ParseToDate` for
  `to_date`, and Spark 4's `try_to_timestamp`)

`Cast` between strings and date / timestamp also reads the policy via the default
formatters but is tested separately by `CometCastSuite` and is out of scope here.

**Comet status.** None of the listed expressions consult `legacyTimeParserPolicy` in
their Comet serde. The native implementations of `date_format`, `from_unixtime`, and
`unix_timestamp` use a fixed strftime-style mapping that does not vary with policy;
the remaining four (`to_unix_timestamp`, `to_timestamp`, `to_date`,
`try_to_timestamp`) have no native implementation and fall back to Spark. Today this
works because:

- `date_format` is `Compatible` only for a small whitelist of formats under UTC; the
  whitelisted formats happen to produce identical output under all three policies.
- `from_unixtime` is marked `Incompatible` and falls back unless
  `spark.comet.expression.FromUnixTime.allowIncompatible=true` is set.
- `unix_timestamp(<timestamp_or_date>)` does not call the formatter at all; the
  string-input overload falls back.

If a Comet contributor adds native string-format parsing or extends the date_format
whitelist, this audit should be revisited and the policy must be honored explicitly.

**Test coverage.** `spark/src/test/resources/sql-tests/expressions/datetime/`:

- One ConfigMatrix file per expression covering convergent inputs under
  `LEGACY,CORRECTED,EXCEPTION` (`*_time_parser_policy.sql`).
- Per-policy files locking in divergent behavior:
  - `_legacy.sql` -- lenient inputs (single-digit fields, out-of-range values,
    trailing characters) and legacy-only pattern tokens (`'aaaa'`).
  - `_corrected.sql` -- the same inputs return null; legacy-only tokens raise
    `INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION` at formatter
    creation.
  - `_exception.sql` -- the same inputs raise
    `INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER` at parse time.

**Findings.** All 42 generated test cases pass on Spark 3.4.3, 3.5.8, and 4.0.1. No
Comet bugs were uncovered by the audit. The tests use `query spark_answer_only` so
that result-correctness is enforced regardless of whether Comet runs the expression
natively or falls back.
