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

<!--
  TO MODIFY THIS CONTENT MAKE SURE THAT YOU MAKE YOUR CHANGES TO THE TEMPLATE FILE
  (docs/templates/compatibility-template.md) AND NOT THE GENERATED FILE
  (docs/source/user-guide/compatibility.md) OTHERWISE YOUR CHANGES MAY BE LOST
-->

# Compatibility Guide

Comet aims to provide consistent results with the version of Apache Spark that is being used.

This guide offers information about areas of functionality where there are known differences.

# Compatibility Guide

Comet aims to provide consistent results with the version of Apache Spark that is being used.

This guide offers information about areas of functionality where there are known differences.

## Parquet Scans

Comet currently has three distinct implementations of the Parquet scan operator. The configuration property
`spark.comet.scan.impl` is used to select an implementation.

| Implementation          | Description                                                                                                                                                                               |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `native_comet`          | This is the default implementation. It provides strong compatibility with Spark but does not support complex types.                                                                       |
| `native_datafusion`     | This implementation delegates to DataFusion's `DataSourceExec`.                                                                                                                           |
| `native_iceberg_compat` | This implementation also delegates to DataFusion's `DataSourceExec` but uses a hybrid approach of JVM and native code. This scan is designed to be integrated with Iceberg in the future. |

The new (and currently experimental) `native_datafusion` and `native_iceberg_compat` scans provide the following benefits over the `native_comet`
implementation:

- Leverages the DataFusion community's ongoing improvements to `DataSourceExec`
- Provides support for reading complex types (structs, arrays, and maps)
- Removes the use of reusable mutable-buffers in Comet, which is complex to maintain
- Improves performance

The new scans currently have the following limitations:

- When reading Parquet files written by systems other than Spark that contain columns with the logical types `UINT_8`
  or `UINT_16`, Comet will produce different results than Spark because Spark does not preserve or understand these
  logical types. Arrow-based readers, such as DataFusion and Comet do respect these types and read the data as unsigned
  rather than signed. By default, Comet will fall back to Spark when scanning Parquet files containing `byte` or `short`
  types (regardless of the logical type). This behavior can be disabled by setting
  `spark.comet.scan.allowIncompatible=true`.
- Reading legacy INT96 timestamps contained within complex types can produce different results to Spark
- There is a known performance issue when pushing filters down to Parquet. See the [Comet Tuning Guide] for more
  information.
- There are failures in the Spark SQL test suite when enabling these new scans (tracking issues: [#1542] and [#1545]).

[#1545]: https://github.com/apache/datafusion-comet/issues/1545
[#1542]: https://github.com/apache/datafusion-comet/issues/1542
[Comet Tuning Guide]: tuning.md

## ANSI mode

Comet currently ignores ANSI mode in most cases, and therefore can produce different results than Spark. By default,
Comet will fall back to Spark if ANSI mode is enabled. To enable Comet to accelerate queries when ANSI mode is enabled,
specify `spark.comet.ansi.enabled=true` in the Spark configuration. Comet's ANSI support is experimental and should not
be used in production.

There is an [epic](https://github.com/apache/datafusion-comet/issues/313) where we are tracking the work to fully implement ANSI support.

## Floating number comparison

Spark normalizes NaN and zero for floating point numbers for several cases. See `NormalizeFloatingNumbers` optimization rule in Spark.
However, one exception is comparison. Spark does not normalize NaN and zero when comparing values
because they are handled well in Spark (e.g., `SQLOrderingUtil.compareFloats`). But the comparison
functions of arrow-rs used by DataFusion do not normalize NaN and zero (e.g., [arrow::compute::kernels::cmp::eq](https://docs.rs/arrow/latest/arrow/compute/kernels/cmp/fn.eq.html#)).
So Comet will add additional normalization expression of NaN and zero for comparison.

## Incompatible Expressions

Some Comet native expressions are not 100% compatible with Spark and are disabled by default. These expressions
will fall back to Spark but can be enabled by setting `spark.comet.expression.allowIncompatible=true`.

## Array Expressions

Comet has experimental support for a number of array expressions. These are experimental and currently marked
as incompatible and can be enabled by setting `spark.comet.expression.allowIncompatible=true`.

## Regular Expressions

Comet uses the Rust regexp crate for evaluating regular expressions, and this has different behavior from Java's
regular expression engine. Comet will fall back to Spark for patterns that are known to produce different results, but
this can be overridden by setting `spark.comet.regexp.allowIncompatible=true`.

## Cast

Cast operations in Comet fall into three levels of support:

- **Compatible**: The results match Apache Spark
- **Incompatible**: The results may match Apache Spark for some inputs, but there are known issues where some inputs
  will result in incorrect results or exceptions. The query stage will fall back to Spark by default. Setting
  `spark.comet.cast.allowIncompatible=true` will allow all incompatible casts to run natively in Comet, but this is not
  recommended for production use.
- **Unsupported**: Comet does not provide a native version of this cast expression and the query stage will fall back to
  Spark.

### Compatible Casts

The following cast operations are generally compatible with Spark except for the differences noted here.

<!--COMPAT_CAST_TABLE-->

### Incompatible Casts

The following cast operations are not compatible with Spark for all inputs and are disabled by default.

<!--INCOMPAT_CAST_TABLE-->

### Unsupported Casts

Any cast not listed in the previous tables is currently unsupported. We are working on adding more. See the
[tracking issue](https://github.com/apache/datafusion-comet/issues/286) for more details.
