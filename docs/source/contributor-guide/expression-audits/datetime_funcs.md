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

# datetime_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## curdate

- Alias of `current_date`; constant-folded to a literal by Spark's `ComputeCurrentTime` rule before Comet sees the plan.

## current_date

- Constant-folded to a literal by Spark's `ComputeCurrentTime` rule before Comet sees the plan.

## current_timestamp

- Constant-folded to a literal by Spark's `ComputeCurrentTime` rule before Comet sees the plan.

## dayname

- Spark 4.0+. Implemented natively: maps a `DateType` value to a fixed US-English abbreviated day name (`DayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US)`), with no session-locale or timezone dependence.

## from_utc_timestamp

- Spark 3.4.3 (audited 2026-05-12): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-12): baseline.
- Spark 4.0.1 (audited 2026-05-12): `inputTypes` widened to `StringTypeWithCollation`; behaviour unchanged for ASCII timezone strings.
- Known divergence: Comet's native timezone parser does not accept Spark's legacy zone forms (`GMT+1`, `UTC+1`, three-letter abbreviations like `PST`). Such timezones throw a native parse error at execution.

## make_timestamp_ltz

- The 6-argument form rewrites to `MakeTimestamp` and runs via the codegen dispatcher. The 2-argument `(date, time)` form requires the Spark 4.1 TIME type and falls back.

## make_timestamp_ntz

- The 6-argument form rewrites to `MakeTimestamp` and runs via the codegen dispatcher. The 2-argument `(date, time)` form requires the Spark 4.1 TIME type and falls back.

## monthname

- Spark 4.0+. Implemented natively: maps a `DateType` value to a fixed US-English abbreviated month name (`Month.getDisplayName(TextStyle.SHORT, Locale.US)`), with no session-locale or timezone dependence.

## now

- Alias of `current_timestamp`; constant-folded to a literal by Spark's `ComputeCurrentTime` rule before Comet sees the plan.

## to_date

- Rewrites to `Cast` (no format, native) or `Cast(GetTimestamp(...))` (with format, via the codegen dispatcher) before Comet sees the plan.

## to_timestamp

- Rewrites to `Cast` (no format, native) or `GetTimestamp` (with format, via the codegen dispatcher) before Comet sees the plan.

## to_timestamp_ltz

- Rewrites to `to_timestamp` with `TimestampType`; same support as `to_timestamp`.

## to_timestamp_ntz

- Rewrites to `to_timestamp` with `TimestampNTZType`; same support as `to_timestamp`.

## to_utc_timestamp

- Spark 3.4.3 (audited 2026-05-12): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-12): baseline.
- Spark 4.0.1 (audited 2026-05-12): `inputTypes` widened to `StringTypeWithCollation`; behaviour unchanged for ASCII timezone strings.
- Known divergence: Comet's native timezone parser does not accept Spark's legacy zone forms (`GMT+1`, `UTC+1`, three-letter abbreviations like `PST`). Such timezones throw a native parse error at execution.

## try_make_timestamp

- Native for valid inputs; returns wrong values for invalid inputs instead of NULL (https://github.com/apache/datafusion-comet/issues/4554).

[Spark Expression Support]: ../../user-guide/latest/expressions.md
