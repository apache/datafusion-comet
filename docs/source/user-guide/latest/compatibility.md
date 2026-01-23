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

# Compatibility Guide

Comet aims to provide consistent results with the version of Apache Spark that is being used.

This guide offers information about areas of functionality where there are known differences.

## Parquet

Comet has the following limitations when reading Parquet files:

- Comet does not support reading decimals encoded in binary format.
- No support for default values that are nested types (e.g., maps, arrays, structs). Literal default values are supported.

## ANSI Mode

Comet will fall back to Spark for the following expressions when ANSI mode is enabled. These expressions can be enabled by setting
`spark.comet.expression.EXPRNAME.allowIncompatible=true`, where `EXPRNAME` is the Spark expression class name. See
the [Comet Supported Expressions Guide](expressions.md) for more information on this configuration setting.

- Average (supports all numeric inputs except decimal types)
- Cast (in some cases)

There is an [epic](https://github.com/apache/datafusion-comet/issues/313) where we are tracking the work to fully implement ANSI support.

## Floating-point Number Comparison

Spark normalizes NaN and zero for floating point numbers for several cases. See `NormalizeFloatingNumbers` optimization rule in Spark.
However, one exception is comparison. Spark does not normalize NaN and zero when comparing values
because they are handled well in Spark (e.g., `SQLOrderingUtil.compareFloats`). But the comparison
functions of arrow-rs used by DataFusion do not normalize NaN and zero (e.g., [arrow::compute::kernels::cmp::eq](https://docs.rs/arrow/latest/arrow/compute/kernels/cmp/fn.eq.html#)).
So Comet adds additional normalization expression of NaN and zero for comparisons, and may still have differences
to Spark in some cases, especially when the data contains both positive and negative zero. This is likely an edge
case that is not of concern for many users. If it is a concern, setting `spark.comet.exec.strictFloatingPoint=true`
will make relevant operations fall back to Spark.

## Incompatible Expressions

Expressions that are not 100% Spark-compatible will fall back to Spark by default and can be enabled by setting
`spark.comet.expression.EXPRNAME.allowIncompatible=true`, where `EXPRNAME` is the Spark expression class name. See
the [Comet Supported Expressions Guide](expressions.md) for more information on this configuration setting.

## Regular Expressions

Comet uses the Rust regexp crate for evaluating regular expressions, and this has different behavior from Java's
regular expression engine. Comet will fall back to Spark for patterns that are known to produce different results, but
this can be overridden by setting `spark.comet.regexp.allowIncompatible=true`.

## Window Functions

Comet's support for window functions is incomplete and known to be incorrect. It is disabled by default and
should not be used in production. The feature will be enabled in a future release. Tracking issue: [#2721](https://github.com/apache/datafusion-comet/issues/2721).

## Cast

Cast operations in Comet fall into three levels of support:

- **C (Compatible)**: The results match Apache Spark
- **I (Incompatible)**: The results may match Apache Spark for some inputs, but there are known issues where some inputs
  will result in incorrect results or exceptions. The query stage will fall back to Spark by default. Setting
  `spark.comet.expression.Cast.allowIncompatible=true` will allow all incompatible casts to run natively in Comet, but this is not
  recommended for production use.
- **U (Unsupported)**: Comet does not provide a native version of this cast expression and the query stage will fall back to
  Spark.
- **N/A**: Spark does not support this cast.

### Legacy Mode

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->

<!--BEGIN:CAST_LEGACY_TABLE-->
<!-- prettier-ignore-start -->
| | binary | boolean | byte | date | decimal | double | float | integer | long | short | string | timestamp |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| binary | - | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | C | N/A |
| boolean | N/A | - | C | N/A | U | C | C | C | C | C | C | U |
| byte | U | C | - | N/A | C | C | C | C | C | C | C | U |
| date | N/A | U | U | - | U | U | U | U | U | U | C | U |
| decimal | N/A | C | C | N/A | - | C | C | C | C | C | C | U |
| double | N/A | C | C | N/A | I | - | C | C | C | C | C | U |
| float | N/A | C | C | N/A | I | C | - | C | C | C | C | U |
| integer | U | C | C | N/A | C | C | C | - | C | C | C | U |
| long | U | C | C | N/A | C | C | C | C | - | C | C | U |
| short | U | C | C | N/A | C | C | C | C | C | - | C | U |
| string | C | C | C | C | I | C | C | C | C | C | - | I |
| timestamp | N/A | U | U | C | U | U | U | U | C | U | C | - |
<!-- prettier-ignore-end -->

**Notes:**

- **decimal -> string**: There can be formatting differences in some case due to Spark using scientific notation where Comet does not
- **double -> decimal**: There can be rounding differences
- **double -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **float -> decimal**: There can be rounding differences
- **float -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **string -> date**: Only supports years between 262143 BC and 262142 AD
- **string -> decimal**: Does not support fullwidth unicode digits (e.g \\uFF10)
  or strings containing null bytes (e.g \\u0000)
- **string -> timestamp**: Not all valid formats are supported
<!--END:CAST_LEGACY_TABLE-->

### Try Mode

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->

<!--BEGIN:CAST_TRY_TABLE-->
<!-- prettier-ignore-start -->
| | binary | boolean | byte | date | decimal | double | float | integer | long | short | string | timestamp |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| binary | - | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | C | N/A |
| boolean | N/A | - | C | N/A | U | C | C | C | C | C | C | U |
| byte | U | C | - | N/A | C | C | C | C | C | C | C | U |
| date | N/A | U | U | - | U | U | U | U | U | U | C | U |
| decimal | N/A | C | C | N/A | - | C | C | C | C | C | C | U |
| double | N/A | C | C | N/A | I | - | C | C | C | C | C | U |
| float | N/A | C | C | N/A | I | C | - | C | C | C | C | U |
| integer | U | C | C | N/A | C | C | C | - | C | C | C | U |
| long | U | C | C | N/A | C | C | C | C | - | C | C | U |
| short | U | C | C | N/A | C | C | C | C | C | - | C | U |
| string | C | C | C | C | I | C | C | C | C | C | - | I |
| timestamp | N/A | U | U | C | U | U | U | U | C | U | C | - |
<!-- prettier-ignore-end -->

**Notes:**

- **decimal -> string**: There can be formatting differences in some case due to Spark using scientific notation where Comet does not
- **double -> decimal**: There can be rounding differences
- **double -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **float -> decimal**: There can be rounding differences
- **float -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **string -> date**: Only supports years between 262143 BC and 262142 AD
- **string -> decimal**: Does not support fullwidth unicode digits (e.g \\uFF10)
  or strings containing null bytes (e.g \\u0000)
- **string -> timestamp**: Not all valid formats are supported
<!--END:CAST_TRY_TABLE-->

### ANSI Mode

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->

<!--BEGIN:CAST_ANSI_TABLE-->
<!-- prettier-ignore-start -->
| | binary | boolean | byte | date | decimal | double | float | integer | long | short | string | timestamp |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| binary | - | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | C | N/A |
| boolean | N/A | - | C | N/A | U | C | C | C | C | C | C | U |
| byte | U | C | - | N/A | C | C | C | C | C | C | C | U |
| date | N/A | U | U | - | U | U | U | U | U | U | C | U |
| decimal | N/A | C | C | N/A | - | C | C | C | C | C | C | U |
| double | N/A | C | C | N/A | I | - | C | C | C | C | C | U |
| float | N/A | C | C | N/A | I | C | - | C | C | C | C | U |
| integer | U | C | C | N/A | C | C | C | - | C | C | C | U |
| long | U | C | C | N/A | C | C | C | C | - | C | C | U |
| short | U | C | C | N/A | C | C | C | C | C | - | C | U |
| string | C | C | C | C | I | C | C | C | C | C | - | I |
| timestamp | N/A | U | U | C | U | U | U | U | C | U | C | - |
<!-- prettier-ignore-end -->

**Notes:**

- **decimal -> string**: There can be formatting differences in some case due to Spark using scientific notation where Comet does not
- **double -> decimal**: There can be rounding differences
- **double -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **float -> decimal**: There can be rounding differences
- **float -> string**: There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45
- **string -> date**: Only supports years between 262143 BC and 262142 AD
- **string -> decimal**: Does not support fullwidth unicode digits (e.g \\uFF10)
  or strings containing null bytes (e.g \\u0000)
- **string -> timestamp**: ANSI mode not supported
<!--END:CAST_ANSI_TABLE-->

See the [tracking issue](https://github.com/apache/datafusion-comet/issues/286) for more details.
