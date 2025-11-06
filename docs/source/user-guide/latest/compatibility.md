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

Comet will fall back to Spark for the following expressions when ANSI mode is enabled, unless
`spark.comet.expression.allowIncompatible=true`.

- Average
- Sum
- Cast (in some cases)

There is an [epic](https://github.com/apache/datafusion-comet/issues/313) where we are tracking the work to fully implement ANSI support.

## Floating-point Number Comparison

Spark normalizes NaN and zero for floating point numbers for several cases. See `NormalizeFloatingNumbers` optimization rule in Spark.
However, one exception is comparison. Spark does not normalize NaN and zero when comparing values
because they are handled well in Spark (e.g., `SQLOrderingUtil.compareFloats`). But the comparison
functions of arrow-rs used by DataFusion do not normalize NaN and zero (e.g., [arrow::compute::kernels::cmp::eq](https://docs.rs/arrow/latest/arrow/compute/kernels/cmp/fn.eq.html#)).
So Comet will add additional normalization expression of NaN and zero for comparison.

Sorting on floating-point data types (or complex types containing floating-point values) is not compatible with
Spark if the data contains both zero and negative zero. This is likely an edge case that is not of concern for many users
and sorting on floating-point data can be enabled by setting `spark.comet.expression.SortOrder.allowIncompatible=true`.

## Incompatible Expressions

Expressions that are not 100% Spark-compatible will fall back to Spark by default and can be enabled by setting
`spark.comet.expression.EXPRNAME.allowIncompatible=true`, where `EXPRNAME` is the Spark expression class name. See
the [Comet Supported Expressions Guide](expressions.md) for more information on this configuration setting.

It is also possible to specify `spark.comet.expression.allowIncompatible=true` to enable all
incompatible expressions.

## Regular Expressions

Comet uses the Rust regexp crate for evaluating regular expressions, and this has different behavior from Java's
regular expression engine. Comet will fall back to Spark for patterns that are known to produce different results, but
this can be overridden by setting `spark.comet.regexp.allowIncompatible=true`.

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
| string | binary |  |
| string | date | Only supports years between 262143 BC and 262142 AD |
| binary | string |  |
| date | string |  |
| timestamp | long |  |
| timestamp | string |  |
| timestamp | date |  |
<!--END:COMPAT_CAST_TABLE-->

### Incompatible Casts

The following cast operations are not compatible with Spark for all inputs and are disabled by default.

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->

<!--BEGIN:INCOMPAT_CAST_TABLE-->
| From Type | To Type | Notes |
|-|-|-|
| float | decimal  | There can be rounding differences |
| double | decimal  | There can be rounding differences |
| string | float  | Does not support inputs ending with 'd' or 'f'. Does not support 'inf'. Does not support ANSI mode. |
| string | double  | Does not support inputs ending with 'd' or 'f'. Does not support 'inf'. Does not support ANSI mode. |
| string | decimal  | Does not support inputs ending with 'd' or 'f'. Does not support 'inf'. Does not support ANSI mode. Returns 0.0 instead of null if input contains no digits |
| string | timestamp  | Not all valid formats are supported |
<!--END:INCOMPAT_CAST_TABLE-->

### Unsupported Casts

Any cast not listed in the previous tables is currently unsupported. We are working on adding more. See the
[tracking issue](https://github.com/apache/datafusion-comet/issues/286) for more details.
