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

- **Compatible**: The results match Apache Spark
- **Incompatible**: The results may match Apache Spark for some inputs, but there are known issues where some inputs
  will result in incorrect results or exceptions. The query stage will fall back to Spark by default. Setting
  `spark.comet.expression.Cast.allowIncompatible=true` will allow all incompatible casts to run natively in Comet, but this is not
  recommended for production use.
- **Unsupported**: Comet does not provide a native version of this cast expression and the query stage will fall back to
  Spark.

### Compatible Casts

The following cast operations are generally compatible with Spark except for the differences noted here.

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->

<!--BEGIN:COMPAT_CAST_TABLE-->
<!-- prettier-ignore-start -->
| From Type | To Type | Notes |
|-|-|-|
| boolean | byte |  |
| boolean | short |  |
| boolean | integer |  |
| boolean | long |  |
| boolean | float |  |
| boolean | double |  |
| boolean | string |  |
| byte | boolean |  |
| byte | short |  |
| byte | integer |  |
| byte | long |  |
| byte | float |  |
| byte | double |  |
| byte | decimal |  |
| byte | string |  |
| short | boolean |  |
| short | byte |  |
| short | integer |  |
| short | long |  |
| short | float |  |
| short | double |  |
| short | decimal |  |
| short | string |  |
| integer | boolean |  |
| integer | byte |  |
| integer | short |  |
| integer | long |  |
| integer | float |  |
| integer | double |  |
| integer | decimal |  |
| integer | string |  |
| long | boolean |  |
| long | byte |  |
| long | short |  |
| long | integer |  |
| long | float |  |
| long | double |  |
| long | decimal |  |
| long | string |  |
| float | boolean |  |
| float | byte |  |
| float | short |  |
| float | integer |  |
| float | long |  |
| float | double |  |
| float | string | There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45 |
| double | boolean |  |
| double | byte |  |
| double | short |  |
| double | integer |  |
| double | long |  |
| double | float |  |
| double | string | There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45 |
| decimal | boolean |  |
| decimal | byte |  |
| decimal | short |  |
| decimal | integer |  |
| decimal | long |  |
| decimal | float |  |
| decimal | double |  |
| decimal | decimal |  |
| decimal | string | There can be formatting differences in some case due to Spark using scientific notation where Comet does not |
| string | boolean |  |
| string | byte |  |
| string | short |  |
| string | integer |  |
| string | long |  |
| string | float |  |
| string | double |  |
| string | binary |  |
| string | date | Only supports years between 262143 BC and 262142 AD |
| binary | string |  |
| date | string |  |
| timestamp | long |  |
| timestamp | string |  |
| timestamp | date |  |
<!-- prettier-ignore-end -->
<!--END:COMPAT_CAST_TABLE-->

### Incompatible Casts

The following cast operations are not compatible with Spark for all inputs and are disabled by default.

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->

<!--BEGIN:INCOMPAT_CAST_TABLE-->
<!-- prettier-ignore-start -->
| From Type | To Type | Notes |
|-|-|-|
| float | decimal  | There can be rounding differences |
| double | decimal  | There can be rounding differences |
| string | decimal  | Does not support fullwidth unicode digits (e.g \\uFF10)
or strings containing null bytes (e.g \\u0000) |
| string | timestamp  | Not all valid formats are supported |
<!-- prettier-ignore-end -->
<!--END:INCOMPAT_CAST_TABLE-->

### Unsupported Casts

Any cast not listed in the previous tables is currently unsupported. We are working on adding more. See the
[tracking issue](https://github.com/apache/datafusion-comet/issues/286) for more details.

### Cast Support by Eval Mode

The following table shows cast support levels for each evaluation mode (LEGACY, ANSI, TRY).

<!--BEGIN:CAST_EVAL_MODE_TABLE-->
<!-- prettier-ignore-start -->
| From Type | To Type | LEGACY | ANSI | TRY |
|-|-|-|-|-|
| boolean | byte | Compatible | Compatible | Compatible |
| boolean | short | Compatible | Compatible | Compatible |
| boolean | integer | Compatible | Compatible | Compatible |
| boolean | long | Compatible | Compatible | Compatible |
| boolean | float | Compatible | Compatible | Compatible |
| boolean | double | Compatible | Compatible | Compatible |
| boolean | decimal | Unsupported | Unsupported | Unsupported |
| boolean | string | Compatible | Compatible | Compatible |
| boolean | timestamp | Unsupported | Unsupported | Unsupported |
| byte | boolean | Compatible | Compatible | Compatible |
| byte | short | Compatible | Compatible | Compatible |
| byte | integer | Compatible | Compatible | Compatible |
| byte | long | Compatible | Compatible | Compatible |
| byte | float | Compatible | Compatible | Compatible |
| byte | double | Compatible | Compatible | Compatible |
| byte | decimal | Compatible | Compatible | Compatible |
| byte | string | Compatible | Compatible | Compatible |
| byte | binary | Unsupported | Unsupported | Unsupported |
| byte | timestamp | Unsupported | Unsupported | Unsupported |
| short | boolean | Compatible | Compatible | Compatible |
| short | byte | Compatible | Compatible | Compatible |
| short | integer | Compatible | Compatible | Compatible |
| short | long | Compatible | Compatible | Compatible |
| short | float | Compatible | Compatible | Compatible |
| short | double | Compatible | Compatible | Compatible |
| short | decimal | Compatible | Compatible | Compatible |
| short | string | Compatible | Compatible | Compatible |
| short | binary | Unsupported | Unsupported | Unsupported |
| short | timestamp | Unsupported | Unsupported | Unsupported |
| integer | boolean | Compatible | Compatible | Compatible |
| integer | byte | Compatible | Compatible | Compatible |
| integer | short | Compatible | Compatible | Compatible |
| integer | long | Compatible | Compatible | Compatible |
| integer | float | Compatible | Compatible | Compatible |
| integer | double | Compatible | Compatible | Compatible |
| integer | decimal | Compatible | Compatible | Compatible |
| integer | string | Compatible | Compatible | Compatible |
| integer | binary | Unsupported | Unsupported | Unsupported |
| integer | timestamp | Unsupported | Unsupported | Unsupported |
| long | boolean | Compatible | Compatible | Compatible |
| long | byte | Compatible | Compatible | Compatible |
| long | short | Compatible | Compatible | Compatible |
| long | integer | Compatible | Compatible | Compatible |
| long | float | Compatible | Compatible | Compatible |
| long | double | Compatible | Compatible | Compatible |
| long | decimal | Compatible | Compatible | Compatible |
| long | string | Compatible | Compatible | Compatible |
| long | binary | Unsupported | Unsupported | Unsupported |
| long | timestamp | Unsupported | Unsupported | Unsupported |
| float | boolean | Compatible | Compatible | Compatible |
| float | byte | Compatible | Compatible | Compatible |
| float | short | Compatible | Compatible | Compatible |
| float | integer | Compatible | Compatible | Compatible |
| float | long | Compatible | Compatible | Compatible |
| float | double | Compatible | Compatible | Compatible |
| float | decimal | Incompatible | Incompatible | Incompatible |
| float | string | Compatible | Compatible | Compatible |
| float | timestamp | Unsupported | Unsupported | Unsupported |
| double | boolean | Compatible | Compatible | Compatible |
| double | byte | Compatible | Compatible | Compatible |
| double | short | Compatible | Compatible | Compatible |
| double | integer | Compatible | Compatible | Compatible |
| double | long | Compatible | Compatible | Compatible |
| double | float | Compatible | Compatible | Compatible |
| double | decimal | Incompatible | Incompatible | Incompatible |
| double | string | Compatible | Compatible | Compatible |
| double | timestamp | Unsupported | Unsupported | Unsupported |
| decimal | boolean | Compatible | Compatible | Compatible |
| decimal | byte | Compatible | Compatible | Compatible |
| decimal | short | Compatible | Compatible | Compatible |
| decimal | integer | Compatible | Compatible | Compatible |
| decimal | long | Compatible | Compatible | Compatible |
| decimal | float | Compatible | Compatible | Compatible |
| decimal | double | Compatible | Compatible | Compatible |
| decimal | string | Compatible | Compatible | Compatible |
| decimal | timestamp | Unsupported | Unsupported | Unsupported |
| string | boolean | Compatible | Compatible | Compatible |
| string | byte | Compatible | Compatible | Compatible |
| string | short | Compatible | Compatible | Compatible |
| string | integer | Compatible | Compatible | Compatible |
| string | long | Compatible | Compatible | Compatible |
| string | float | Compatible | Compatible | Compatible |
| string | double | Compatible | Compatible | Compatible |
| string | decimal | Incompatible | Incompatible | Incompatible |
| string | binary | Compatible | Compatible | Compatible |
| string | date | Compatible | Compatible | Compatible |
| string | timestamp | Incompatible | Incompatible | Incompatible |
| binary | string | Compatible | Compatible | Compatible |
| date | boolean | Unsupported | Unsupported | Unsupported |
| date | byte | Unsupported | Unsupported | Unsupported |
| date | short | Unsupported | Unsupported | Unsupported |
| date | integer | Unsupported | Unsupported | Unsupported |
| date | long | Unsupported | Unsupported | Unsupported |
| date | float | Unsupported | Unsupported | Unsupported |
| date | double | Unsupported | Unsupported | Unsupported |
| date | decimal | Unsupported | Unsupported | Unsupported |
| date | string | Compatible | Compatible | Compatible |
| date | timestamp | Unsupported | Unsupported | Unsupported |
| timestamp | boolean | Unsupported | Unsupported | Unsupported |
| timestamp | byte | Unsupported | Unsupported | Unsupported |
| timestamp | short | Unsupported | Unsupported | Unsupported |
| timestamp | integer | Unsupported | Unsupported | Unsupported |
| timestamp | long | Compatible | Compatible | Compatible |
| timestamp | float | Unsupported | Unsupported | Unsupported |
| timestamp | double | Unsupported | Unsupported | Unsupported |
| timestamp | decimal | Unsupported | Unsupported | Unsupported |
| timestamp | string | Compatible | Compatible | Compatible |
| timestamp | date | Compatible | Compatible | Compatible |
<!-- prettier-ignore-end -->
<!--END:CAST_EVAL_MODE_TABLE-->
