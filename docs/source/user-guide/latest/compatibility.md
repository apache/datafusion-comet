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

The following tables show cast support levels for each evaluation mode (LEGACY, TRY, ANSI).

#### LEGACY Mode

<!--BEGIN:CAST_LEGACY_TABLE-->
<!-- prettier-ignore-start -->
| From Type | To Type | Support | Notes |
|-|-|-|-|
| binary | string | Compatible |  |
| boolean | byte | Compatible |  |
| boolean | decimal | Unsupported | Cast from BooleanType to DecimalType is not supported |
| boolean | double | Compatible |  |
| boolean | float | Compatible |  |
| boolean | integer | Compatible |  |
| boolean | long | Compatible |  |
| boolean | short | Compatible |  |
| boolean | string | Compatible |  |
| boolean | timestamp | Unsupported | Cast from BooleanType to TimestampType is not supported |
| byte | binary | Unsupported | Cast from ByteType to BinaryType is not supported |
| byte | boolean | Compatible |  |
| byte | decimal | Compatible |  |
| byte | double | Compatible |  |
| byte | float | Compatible |  |
| byte | integer | Compatible |  |
| byte | long | Compatible |  |
| byte | short | Compatible |  |
| byte | string | Compatible |  |
| byte | timestamp | Unsupported | Cast from ByteType to TimestampType is not supported |
| date | boolean | Unsupported | Cast from DateType to BooleanType is not supported |
| date | byte | Unsupported | Cast from DateType to ByteType is not supported |
| date | decimal | Unsupported | Cast from DateType to DecimalType is not supported |
| date | double | Unsupported | Cast from DateType to DoubleType is not supported |
| date | float | Unsupported | Cast from DateType to FloatType is not supported |
| date | integer | Unsupported | Cast from DateType to IntegerType is not supported |
| date | long | Unsupported | Cast from DateType to LongType is not supported |
| date | short | Unsupported | Cast from DateType to ShortType is not supported |
| date | string | Compatible |  |
| date | timestamp | Unsupported | Cast from DateType to TimestampType is not supported |
| decimal | boolean | Compatible |  |
| decimal | byte | Compatible |  |
| decimal | double | Compatible |  |
| decimal | float | Compatible |  |
| decimal | integer | Compatible |  |
| decimal | long | Compatible |  |
| decimal | short | Compatible |  |
| decimal | string | Compatible | There can be formatting differences in some case due to Spark using scientific notation where Comet does not |
| decimal | timestamp | Unsupported | Cast from DecimalType to TimestampType is not supported |
| double | boolean | Compatible |  |
| double | byte | Compatible |  |
| double | decimal | Incompatible | There can be rounding differences |
| double | float | Compatible |  |
| double | integer | Compatible |  |
| double | long | Compatible |  |
| double | short | Compatible |  |
| double | string | Compatible | There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45 |
| double | timestamp | Unsupported | Cast from DoubleType to TimestampType is not supported |
| float | boolean | Compatible |  |
| float | byte | Compatible |  |
| float | decimal | Incompatible | There can be rounding differences |
| float | double | Compatible |  |
| float | integer | Compatible |  |
| float | long | Compatible |  |
| float | short | Compatible |  |
| float | string | Compatible | There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45 |
| float | timestamp | Unsupported | Cast from FloatType to TimestampType is not supported |
| integer | binary | Unsupported | Cast from IntegerType to BinaryType is not supported |
| integer | boolean | Compatible |  |
| integer | byte | Compatible |  |
| integer | decimal | Compatible |  |
| integer | double | Compatible |  |
| integer | float | Compatible |  |
| integer | long | Compatible |  |
| integer | short | Compatible |  |
| integer | string | Compatible |  |
| integer | timestamp | Unsupported | Cast from IntegerType to TimestampType is not supported |
| long | binary | Unsupported | Cast from LongType to BinaryType is not supported |
| long | boolean | Compatible |  |
| long | byte | Compatible |  |
| long | decimal | Compatible |  |
| long | double | Compatible |  |
| long | float | Compatible |  |
| long | integer | Compatible |  |
| long | short | Compatible |  |
| long | string | Compatible |  |
| long | timestamp | Unsupported | Cast from LongType to TimestampType is not supported |
| short | binary | Unsupported | Cast from ShortType to BinaryType is not supported |
| short | boolean | Compatible |  |
| short | byte | Compatible |  |
| short | decimal | Compatible |  |
| short | double | Compatible |  |
| short | float | Compatible |  |
| short | integer | Compatible |  |
| short | long | Compatible |  |
| short | string | Compatible |  |
| short | timestamp | Unsupported | Cast from ShortType to TimestampType is not supported |
| string | binary | Compatible |  |
| string | boolean | Compatible |  |
| string | byte | Compatible |  |
| string | date | Compatible | Only supports years between 262143 BC and 262142 AD |
| string | decimal | Incompatible | Does not support fullwidth unicode digits (e.g \\uFF10)
or strings containing null bytes (e.g \\u0000) |
| string | double | Compatible |  |
| string | float | Compatible |  |
| string | integer | Compatible |  |
| string | long | Compatible |  |
| string | short | Compatible |  |
| string | timestamp | Incompatible | Not all valid formats are supported |
| timestamp | boolean | Unsupported | Cast from TimestampType to BooleanType is not supported |
| timestamp | byte | Unsupported | Cast from TimestampType to ByteType is not supported |
| timestamp | date | Compatible |  |
| timestamp | decimal | Unsupported | Cast from TimestampType to DecimalType is not supported |
| timestamp | double | Unsupported | Cast from TimestampType to DoubleType is not supported |
| timestamp | float | Unsupported | Cast from TimestampType to FloatType is not supported |
| timestamp | integer | Unsupported | Cast from TimestampType to IntegerType is not supported |
| timestamp | long | Compatible |  |
| timestamp | short | Unsupported | Cast from TimestampType to ShortType is not supported |
| timestamp | string | Compatible |  |
<!-- prettier-ignore-end -->
<!--END:CAST_LEGACY_TABLE-->

#### TRY Mode

<!--BEGIN:CAST_TRY_TABLE-->
<!-- prettier-ignore-start -->
| From Type | To Type | Support | Notes |
|-|-|-|-|
| binary | string | Compatible |  |
| boolean | byte | Compatible |  |
| boolean | decimal | Unsupported | Cast from BooleanType to DecimalType is not supported |
| boolean | double | Compatible |  |
| boolean | float | Compatible |  |
| boolean | integer | Compatible |  |
| boolean | long | Compatible |  |
| boolean | short | Compatible |  |
| boolean | string | Compatible |  |
| boolean | timestamp | Unsupported | Cast from BooleanType to TimestampType is not supported |
| byte | binary | Unsupported | Cast from ByteType to BinaryType is not supported |
| byte | boolean | Compatible |  |
| byte | decimal | Compatible |  |
| byte | double | Compatible |  |
| byte | float | Compatible |  |
| byte | integer | Compatible |  |
| byte | long | Compatible |  |
| byte | short | Compatible |  |
| byte | string | Compatible |  |
| byte | timestamp | Unsupported | Cast from ByteType to TimestampType is not supported |
| date | boolean | Unsupported | Cast from DateType to BooleanType is not supported |
| date | byte | Unsupported | Cast from DateType to ByteType is not supported |
| date | decimal | Unsupported | Cast from DateType to DecimalType is not supported |
| date | double | Unsupported | Cast from DateType to DoubleType is not supported |
| date | float | Unsupported | Cast from DateType to FloatType is not supported |
| date | integer | Unsupported | Cast from DateType to IntegerType is not supported |
| date | long | Unsupported | Cast from DateType to LongType is not supported |
| date | short | Unsupported | Cast from DateType to ShortType is not supported |
| date | string | Compatible |  |
| date | timestamp | Unsupported | Cast from DateType to TimestampType is not supported |
| decimal | boolean | Compatible |  |
| decimal | byte | Compatible |  |
| decimal | double | Compatible |  |
| decimal | float | Compatible |  |
| decimal | integer | Compatible |  |
| decimal | long | Compatible |  |
| decimal | short | Compatible |  |
| decimal | string | Compatible | There can be formatting differences in some case due to Spark using scientific notation where Comet does not |
| decimal | timestamp | Unsupported | Cast from DecimalType to TimestampType is not supported |
| double | boolean | Compatible |  |
| double | byte | Compatible |  |
| double | decimal | Incompatible | There can be rounding differences |
| double | float | Compatible |  |
| double | integer | Compatible |  |
| double | long | Compatible |  |
| double | short | Compatible |  |
| double | string | Compatible | There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45 |
| double | timestamp | Unsupported | Cast from DoubleType to TimestampType is not supported |
| float | boolean | Compatible |  |
| float | byte | Compatible |  |
| float | decimal | Incompatible | There can be rounding differences |
| float | double | Compatible |  |
| float | integer | Compatible |  |
| float | long | Compatible |  |
| float | short | Compatible |  |
| float | string | Compatible | There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45 |
| float | timestamp | Unsupported | Cast from FloatType to TimestampType is not supported |
| integer | binary | Unsupported | Cast from IntegerType to BinaryType is not supported |
| integer | boolean | Compatible |  |
| integer | byte | Compatible |  |
| integer | decimal | Compatible |  |
| integer | double | Compatible |  |
| integer | float | Compatible |  |
| integer | long | Compatible |  |
| integer | short | Compatible |  |
| integer | string | Compatible |  |
| integer | timestamp | Unsupported | Cast from IntegerType to TimestampType is not supported |
| long | binary | Unsupported | Cast from LongType to BinaryType is not supported |
| long | boolean | Compatible |  |
| long | byte | Compatible |  |
| long | decimal | Compatible |  |
| long | double | Compatible |  |
| long | float | Compatible |  |
| long | integer | Compatible |  |
| long | short | Compatible |  |
| long | string | Compatible |  |
| long | timestamp | Unsupported | Cast from LongType to TimestampType is not supported |
| short | binary | Unsupported | Cast from ShortType to BinaryType is not supported |
| short | boolean | Compatible |  |
| short | byte | Compatible |  |
| short | decimal | Compatible |  |
| short | double | Compatible |  |
| short | float | Compatible |  |
| short | integer | Compatible |  |
| short | long | Compatible |  |
| short | string | Compatible |  |
| short | timestamp | Unsupported | Cast from ShortType to TimestampType is not supported |
| string | binary | Compatible |  |
| string | boolean | Compatible |  |
| string | byte | Compatible |  |
| string | date | Compatible | Only supports years between 262143 BC and 262142 AD |
| string | decimal | Incompatible | Does not support fullwidth unicode digits (e.g \\uFF10)
or strings containing null bytes (e.g \\u0000) |
| string | double | Compatible |  |
| string | float | Compatible |  |
| string | integer | Compatible |  |
| string | long | Compatible |  |
| string | short | Compatible |  |
| string | timestamp | Incompatible | Not all valid formats are supported |
| timestamp | boolean | Unsupported | Cast from TimestampType to BooleanType is not supported |
| timestamp | byte | Unsupported | Cast from TimestampType to ByteType is not supported |
| timestamp | date | Compatible |  |
| timestamp | decimal | Unsupported | Cast from TimestampType to DecimalType is not supported |
| timestamp | double | Unsupported | Cast from TimestampType to DoubleType is not supported |
| timestamp | float | Unsupported | Cast from TimestampType to FloatType is not supported |
| timestamp | integer | Unsupported | Cast from TimestampType to IntegerType is not supported |
| timestamp | long | Compatible |  |
| timestamp | short | Unsupported | Cast from TimestampType to ShortType is not supported |
| timestamp | string | Compatible |  |
<!-- prettier-ignore-end -->
<!--END:CAST_TRY_TABLE-->

#### ANSI Mode

<!--BEGIN:CAST_ANSI_TABLE-->
<!-- prettier-ignore-start -->
| From Type | To Type | Support | Notes |
|-|-|-|-|
| binary | string | Compatible |  |
| boolean | byte | Compatible |  |
| boolean | decimal | Unsupported | Cast from BooleanType to DecimalType is not supported |
| boolean | double | Compatible |  |
| boolean | float | Compatible |  |
| boolean | integer | Compatible |  |
| boolean | long | Compatible |  |
| boolean | short | Compatible |  |
| boolean | string | Compatible |  |
| boolean | timestamp | Unsupported | Cast from BooleanType to TimestampType is not supported |
| byte | binary | Unsupported | Cast from ByteType to BinaryType is not supported |
| byte | boolean | Compatible |  |
| byte | decimal | Compatible |  |
| byte | double | Compatible |  |
| byte | float | Compatible |  |
| byte | integer | Compatible |  |
| byte | long | Compatible |  |
| byte | short | Compatible |  |
| byte | string | Compatible |  |
| byte | timestamp | Unsupported | Cast from ByteType to TimestampType is not supported |
| date | boolean | Unsupported | Cast from DateType to BooleanType is not supported |
| date | byte | Unsupported | Cast from DateType to ByteType is not supported |
| date | decimal | Unsupported | Cast from DateType to DecimalType is not supported |
| date | double | Unsupported | Cast from DateType to DoubleType is not supported |
| date | float | Unsupported | Cast from DateType to FloatType is not supported |
| date | integer | Unsupported | Cast from DateType to IntegerType is not supported |
| date | long | Unsupported | Cast from DateType to LongType is not supported |
| date | short | Unsupported | Cast from DateType to ShortType is not supported |
| date | string | Compatible |  |
| date | timestamp | Unsupported | Cast from DateType to TimestampType is not supported |
| decimal | boolean | Compatible |  |
| decimal | byte | Compatible |  |
| decimal | double | Compatible |  |
| decimal | float | Compatible |  |
| decimal | integer | Compatible |  |
| decimal | long | Compatible |  |
| decimal | short | Compatible |  |
| decimal | string | Compatible | There can be formatting differences in some case due to Spark using scientific notation where Comet does not |
| decimal | timestamp | Unsupported | Cast from DecimalType to TimestampType is not supported |
| double | boolean | Compatible |  |
| double | byte | Compatible |  |
| double | decimal | Incompatible | There can be rounding differences |
| double | float | Compatible |  |
| double | integer | Compatible |  |
| double | long | Compatible |  |
| double | short | Compatible |  |
| double | string | Compatible | There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45 |
| double | timestamp | Unsupported | Cast from DoubleType to TimestampType is not supported |
| float | boolean | Compatible |  |
| float | byte | Compatible |  |
| float | decimal | Incompatible | There can be rounding differences |
| float | double | Compatible |  |
| float | integer | Compatible |  |
| float | long | Compatible |  |
| float | short | Compatible |  |
| float | string | Compatible | There can be differences in precision. For example, the input "1.4E-45" will produce 1.0E-45 instead of 1.4E-45 |
| float | timestamp | Unsupported | Cast from FloatType to TimestampType is not supported |
| integer | binary | Unsupported | Cast from IntegerType to BinaryType is not supported |
| integer | boolean | Compatible |  |
| integer | byte | Compatible |  |
| integer | decimal | Compatible |  |
| integer | double | Compatible |  |
| integer | float | Compatible |  |
| integer | long | Compatible |  |
| integer | short | Compatible |  |
| integer | string | Compatible |  |
| integer | timestamp | Unsupported | Cast from IntegerType to TimestampType is not supported |
| long | binary | Unsupported | Cast from LongType to BinaryType is not supported |
| long | boolean | Compatible |  |
| long | byte | Compatible |  |
| long | decimal | Compatible |  |
| long | double | Compatible |  |
| long | float | Compatible |  |
| long | integer | Compatible |  |
| long | short | Compatible |  |
| long | string | Compatible |  |
| long | timestamp | Unsupported | Cast from LongType to TimestampType is not supported |
| short | binary | Unsupported | Cast from ShortType to BinaryType is not supported |
| short | boolean | Compatible |  |
| short | byte | Compatible |  |
| short | decimal | Compatible |  |
| short | double | Compatible |  |
| short | float | Compatible |  |
| short | integer | Compatible |  |
| short | long | Compatible |  |
| short | string | Compatible |  |
| short | timestamp | Unsupported | Cast from ShortType to TimestampType is not supported |
| string | binary | Compatible |  |
| string | boolean | Compatible |  |
| string | byte | Compatible |  |
| string | date | Compatible | Only supports years between 262143 BC and 262142 AD |
| string | decimal | Incompatible | Does not support fullwidth unicode digits (e.g \\uFF10)
or strings containing null bytes (e.g \\u0000) |
| string | double | Compatible |  |
| string | float | Compatible |  |
| string | integer | Compatible |  |
| string | long | Compatible |  |
| string | short | Compatible |  |
| string | timestamp | Incompatible | ANSI mode not supported |
| timestamp | boolean | Unsupported | Cast from TimestampType to BooleanType is not supported |
| timestamp | byte | Unsupported | Cast from TimestampType to ByteType is not supported |
| timestamp | date | Compatible |  |
| timestamp | decimal | Unsupported | Cast from TimestampType to DecimalType is not supported |
| timestamp | double | Unsupported | Cast from TimestampType to DoubleType is not supported |
| timestamp | float | Unsupported | Cast from TimestampType to FloatType is not supported |
| timestamp | integer | Unsupported | Cast from TimestampType to IntegerType is not supported |
| timestamp | long | Compatible |  |
| timestamp | short | Unsupported | Cast from TimestampType to ShortType is not supported |
| timestamp | string | Compatible |  |
<!-- prettier-ignore-end -->
<!--END:CAST_ANSI_TABLE-->
