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

## String to Timestamp

Comet's native `CAST(string AS TIMESTAMP)` implementation supports all timestamp formats accepted
by Apache Spark, including ISO 8601 date-time strings, date-only strings, time-only strings
(`HH:MM:SS`), embedded timezone offsets (e.g. `+07:30`, `GMT-01:00`, `UTC`), named timezone
suffixes (e.g. `Europe/Moscow`), and the full Spark timestamp year range
(-290308 to 294247). Note that `CAST(string AS DATE)` is only compatible for years between
262143 BC and 262142 AD due to an underlying library limitation.

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
<!--END:CAST_LEGACY_TABLE-->

## Try Mode

<!--BEGIN:CAST_TRY_TABLE-->
<!--END:CAST_TRY_TABLE-->

## ANSI Mode

<!--BEGIN:CAST_ANSI_TABLE-->
<!--END:CAST_ANSI_TABLE-->

See the [tracking issue](https://github.com/apache/datafusion-comet/issues/286) for more details.
