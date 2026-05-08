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

# Cast

Cast operations in Comet fall into three levels of support:

- **C (Compatible)**: The results match Apache Spark
- **I (Incompatible)**: The results may match Apache Spark for some inputs, but there are known issues where some inputs
  will result in incorrect results or exceptions. The query stage will fall back to Spark by default. Setting
  `spark.comet.expression.Cast.allowIncompatible=true` will allow all incompatible casts to run natively in Comet, but this is not
  recommended for production use.
- **U (Unsupported)**: Comet does not provide a native version of this cast expression and the query stage will fall back to
  Spark.
- **N/A**: Spark does not support this cast.

## ANSI Mode Fallback

Cast will fall back to Spark in some cases when ANSI mode is enabled. This can be enabled by setting `spark.comet.expression.Cast.allowIncompatible=true`. See the [Comet Supported Expressions Guide](../../expressions.md) for more information on this configuration setting.

There is an [epic](https://github.com/apache/datafusion-comet/issues/313) where we are tracking the work to fully implement ANSI support.

## String to Decimal

Comet's native `CAST(string AS DECIMAL)` implementation matches Apache Spark's behavior,
including:

- Leading and trailing ASCII whitespace is trimmed before parsing.
- Null bytes (`\u0000`) at the start or end of a string are trimmed, matching Spark's
  `UTF8String` behavior. Null bytes embedded in the middle of a string produce `NULL`.
- Fullwidth Unicode digits (U+FF10–U+FF19, e.g. `１２３.４５`) are treated as their ASCII
  equivalents, so `CAST('１２３.４５' AS DECIMAL(10,2))` returns `123.45`.
- Scientific notation (e.g. `1.23E+5`) is supported.
- Special values (`inf`, `infinity`, `nan`) produce `NULL`.

## String to Date

Comet's native `CAST(string AS DATE)` implementation matches Apache Spark's behavior for years
between 262143 BC and 262142 AD. This range limitation comes from the underlying chrono library's
`NaiveDate` type. Spark itself supports a wider range. All three eval modes (Legacy, ANSI, Try)
are supported.

Supported input formats match Spark exactly:

- `yyyy`, `yyyy-[m]m`, `yyyy-[m]m-[d]d`
- Optional `T` suffix with arbitrary trailing text (e.g. `2020-01-01T12:34:56`)
- Leading/trailing whitespace and control characters are trimmed
- Optional sign prefix (`-` for negative years)
- Leading zeros (e.g. `0002020-01-01` is year 2020)

## Date to Timestamp

Comet's native `CAST(date AS TIMESTAMP)` is compatible with Spark. The cast interprets each
date as midnight in the session timezone and converts to a UTC epoch value. DST transitions
are handled correctly, including spring-forward gaps (where midnight may not exist) and
fall-back ambiguity (where Comet picks the earlier/DST occurrence, matching Spark's
`LocalDate.atStartOfDay(zoneId)` behavior).

## Date to TimestampNTZ

Comet's native `CAST(date AS TIMESTAMP_NTZ)` is compatible with Spark. The cast is
timezone-independent: each date is converted to midnight as pure arithmetic
(`days * 86,400,000,000` microseconds) with no session timezone offset applied. The result
is the same regardless of the session timezone setting.

## Date to Numeric Types

In Legacy mode, `CAST(date AS INT)`, `CAST(date AS LONG)`, and casts to all other numeric
types (Boolean, Byte, Short, Float, Double, Decimal) always return `NULL`. Comet handles
this by short-circuiting to a null literal during query planning, so no native execution
is needed. In ANSI and Try modes, Spark rejects these casts at analysis time (before
execution reaches Comet).

## String to Timestamp

Comet's native `CAST(string AS TIMESTAMP)` implementation supports all timestamp formats accepted
by Apache Spark, including ISO 8601 date-time strings, date-only strings, time-only strings
(`HH:MM:SS`), embedded timezone offsets (e.g. `+07:30`, `GMT-01:00`, `UTC`), named timezone
suffixes (e.g. `Europe/Moscow`), and the full Spark timestamp year range
(-290308 to 294247).

## String to TimestampNTZ

Comet's native `CAST(string AS TIMESTAMP_NTZ)` implementation matches Apache Spark's behavior.
Unlike `CAST(string AS TIMESTAMP)`, this cast is timezone-independent: any timezone offset in
the input string (e.g. `+08:00`, `Z`, `UTC`) is silently discarded, and the local date-time
components are preserved as-is. Time-only strings (e.g. `T12:34:56`, `12:34`) produce `NULL`.
The result is always a wall-clock timestamp with no timezone conversion or DST adjustment.

## TimestampNTZ Casts

Comet supports the following `TIMESTAMP_NTZ` casts natively:

| Cast                               | Compatible | Notes                                                             |
| ---------------------------------- | ---------- | ----------------------------------------------------------------- |
| `CAST(timestamp_ntz AS STRING)`    | Yes        | Formats local time as-is, timezone-independent                    |
| `CAST(timestamp_ntz AS DATE)`      | Yes        | Extracts the date component, timezone-independent                 |
| `CAST(timestamp_ntz AS TIMESTAMP)` | Yes        | Interprets NTZ as local time in session TZ, converts to UTC epoch |
| `CAST(date AS TIMESTAMP_NTZ)`      | Yes        | Pure arithmetic, timezone-independent                             |
| `CAST(timestamp AS TIMESTAMP_NTZ)` | Yes        | Shifts UTC epoch to local time in session TZ                      |
| `CAST(string AS TIMESTAMP_NTZ)`    | Yes        | See [String to TimestampNTZ](#string-to-timestampntz) above       |

The NTZ-to-Timestamp and Timestamp-to-NTZ casts are session-timezone-dependent (the session
timezone determines the UTC offset). All other NTZ casts are timezone-independent and produce
the same result regardless of the session timezone.

## Date to String

Comet's native `CAST(date AS STRING)` is compatible with Spark. Years below 1000 are
zero-padded to four digits (e.g. year 999 renders as `0999-01-01`). Years above 9999 are
rendered without truncation. The cast is timezone-independent.

## String to TimestampNTZ

Comet's native `CAST(string AS TIMESTAMP_NTZ)` implementation matches Apache Spark's behavior.
Unlike `CAST(string AS TIMESTAMP)`, this cast is timezone-independent: any timezone offset in
the input string (e.g. `+08:00`, `Z`, `UTC`) is silently discarded, and the local date-time
components are preserved as-is. Time-only strings (e.g. `T12:34:56`, `12:34`) produce `NULL`.
The result is always a wall-clock timestamp with no timezone conversion or DST adjustment.

## Decimal with Negative Scale to String

Casting a `DecimalType` with a negative scale to `StringType` is marked as incompatible when
`spark.sql.legacy.allowNegativeScaleOfDecimal` is `false` (the default). When that config is
disabled, Spark cannot create negative-scale decimals, so Comet falls back to avoid running
native execution on unexpected inputs.

When `spark.sql.legacy.allowNegativeScaleOfDecimal=true`, the cast is compatible. Comet matches
Spark's behavior of using Java `BigDecimal.toString()` semantics, which produces scientific
notation (e.g. a value of 12300 stored as `Decimal(7,-2)` with unscaled value 123 is rendered
as `"1.23E+4"`).

## Legacy Mode

<!--BEGIN:CAST_LEGACY_TABLE-->
<!-- prettier-ignore-start -->
| | binary | boolean | byte | date | decimal | double | float | integer | long | short | string | timestamp | timestamp_ntz |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| binary | - | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | C | N/A | N/A |
| boolean | N/A | - | C | N/A | C | C | C | C | C | C | C | C | N/A |
| byte | C | C | - | N/A | C | C | C | C | C | C | C | C | N/A |
| date | N/A | C | C | - | C | C | C | C | C | C | C | C | C |
| decimal | N/A | C | C | N/A | - | C | C | C | C | C | C | C | N/A |
| double | N/A | C | C | N/A | I | - | C | C | C | C | C | C | N/A |
| float | N/A | C | C | N/A | I | C | - | C | C | C | C | C | N/A |
| integer | C | C | C | N/A | C | C | C | - | C | C | C | C | N/A |
| long | C | C | C | N/A | C | C | C | C | - | C | C | C | N/A |
| short | C | C | C | N/A | C | C | C | C | C | - | C | C | N/A |
| string | C | C | C | C | C | C | C | C | C | C | - | C | C |
| timestamp | N/A | U | U | C | U | U | U | U | C | U | C | - | C |
| timestamp_ntz | N/A | N/A | N/A | C | N/A | N/A | N/A | N/A | N/A | N/A | C | C | - |

**Notes:**
- **double -> decimal**: There can be rounding differences
- **double -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **float -> decimal**: There can be rounding differences
- **float -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **string -> date**: Only supports years between 262143 BC and 262142 AD
<!-- prettier-ignore-end -->
<!--END:CAST_LEGACY_TABLE-->

## Try Mode

<!--BEGIN:CAST_TRY_TABLE-->
<!-- prettier-ignore-start -->
| | binary | boolean | byte | date | decimal | double | float | integer | long | short | string | timestamp | timestamp_ntz |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| binary | - | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | C | N/A | N/A |
| boolean | N/A | - | C | N/A | C | C | C | C | C | C | C | U | N/A |
| byte | U | C | - | N/A | C | C | C | C | C | C | C | C | N/A |
| date | N/A | U | U | - | U | U | U | U | U | U | C | C | C |
| decimal | N/A | C | C | N/A | - | C | C | C | C | C | C | C | N/A |
| double | N/A | C | C | N/A | I | - | C | C | C | C | C | C | N/A |
| float | N/A | C | C | N/A | I | C | - | C | C | C | C | C | N/A |
| integer | U | C | C | N/A | C | C | C | - | C | C | C | C | N/A |
| long | U | C | C | N/A | C | C | C | C | - | C | C | C | N/A |
| short | U | C | C | N/A | C | C | C | C | C | - | C | C | N/A |
| string | C | C | C | C | C | C | C | C | C | C | - | C | C |
| timestamp | N/A | U | U | C | U | U | U | U | C | U | C | - | C |
| timestamp_ntz | N/A | N/A | N/A | C | N/A | N/A | N/A | N/A | N/A | N/A | C | C | - |

**Notes:**
- **double -> decimal**: There can be rounding differences
- **double -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **float -> decimal**: There can be rounding differences
- **float -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **string -> date**: Only supports years between 262143 BC and 262142 AD
<!-- prettier-ignore-end -->
<!--END:CAST_TRY_TABLE-->

## ANSI Mode

<!--BEGIN:CAST_ANSI_TABLE-->
<!-- prettier-ignore-start -->
| | binary | boolean | byte | date | decimal | double | float | integer | long | short | string | timestamp | timestamp_ntz |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| binary | - | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | C | N/A | N/A |
| boolean | N/A | - | C | N/A | C | C | C | C | C | C | C | U | N/A |
| byte | U | C | - | N/A | C | C | C | C | C | C | C | C | N/A |
| date | N/A | U | U | - | U | U | U | U | U | U | C | C | C |
| decimal | N/A | C | C | N/A | - | C | C | C | C | C | C | C | N/A |
| double | N/A | C | C | N/A | I | - | C | C | C | C | C | C | N/A |
| float | N/A | C | C | N/A | I | C | - | C | C | C | C | C | N/A |
| integer | U | C | C | N/A | C | C | C | - | C | C | C | C | N/A |
| long | U | C | C | N/A | C | C | C | C | - | C | C | C | N/A |
| short | U | C | C | N/A | C | C | C | C | C | - | C | C | N/A |
| string | C | C | C | C | C | C | C | C | C | C | - | C | C |
| timestamp | N/A | U | U | C | U | U | U | U | C | U | C | - | C |
| timestamp_ntz | N/A | N/A | N/A | C | N/A | N/A | N/A | N/A | N/A | N/A | C | C | - |

**Notes:**
- **double -> decimal**: There can be rounding differences
- **double -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **float -> decimal**: There can be rounding differences
- **float -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **string -> date**: Only supports years between 262143 BC and 262142 AD
<!-- prettier-ignore-end -->
<!--END:CAST_ANSI_TABLE-->

See the [tracking issue](https://github.com/apache/datafusion-comet/issues/286) for more details.
