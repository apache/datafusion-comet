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

# Spark Data Type Support

This page is the complete reference for how Apache Comet handles each Spark data type. Comet's
native execution path is built on Apache Arrow, so the set of types Comet can express natively
is constrained by Arrow's type system. When a query references a type Comet does not support,
the relevant operator falls back to Spark; results are unaffected.

For per-scan and per-operator type caveats (for example, Parquet read-time conversions or
hash-aggregate group-key restrictions), see the [Compatibility Guide](compatibility/index.md).

## Status legend

| Status                 | Meaning                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------- |
| ✅ Supported           | Native support; enabled by default.                                                     |
| ⚠️ Supported (caveats) | Works, but with limits: certain values, contexts, or configurations fall back to Spark. |
| 🔜 Planned             | Intended; tracked by an open issue or pull request.                                     |

## Not currently planned

The following types fall back to Spark and are not on the current roadmap. They are omitted from
the tables below and may be reconsidered based on demand:

- **`UserDefinedType`**: user-defined types are application-specific and outside the scope of native acceleration; queries referencing UDTs fall back to Spark.

## Numeric

| Type          | Status | Notes                                                                                                                                                     |
| ------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ByteType`    | ✅     |                                                                                                                                                           |
| `ShortType`   | ✅     |                                                                                                                                                           |
| `IntegerType` | ✅     |                                                                                                                                                           |
| `LongType`    | ✅     |                                                                                                                                                           |
| `FloatType`   | ✅     | NaN and signed-zero handling can diverge from Spark in comparisons and aggregations. See [Floating-point Compatibility](compatibility/floating-point.md). |
| `DoubleType`  | ✅     | NaN and signed-zero handling can diverge from Spark in comparisons and aggregations. See [Floating-point Compatibility](compatibility/floating-point.md). |
| `DecimalType` | ✅     |                                                                                                                                                           |

## String and binary

| Type          | Status | Notes                                                                                                                                                         |
| ------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `StringType`  | ✅     | Default UTF-8 binary collation is supported. Non-default collations (Spark 4.0+) fall back ([#2190](https://github.com/apache/datafusion-comet/issues/2190)). |
| `BinaryType`  | ✅     |                                                                                                                                                               |
| `CharType`    | ✅     | Spark normalizes `CHAR(n)` to `StringType` for evaluation; same caveats apply.                                                                                |
| `VarcharType` | ✅     | Spark normalizes `VARCHAR(n)` to `StringType` for evaluation; same caveats apply.                                                                             |

## Boolean

| Type          | Status | Notes |
| ------------- | ------ | ----- |
| `BooleanType` | ✅     |       |

## Datetime

| Type               | Status | Notes                                                                                                                                                                             |
| ------------------ | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DateType`         | ✅     |                                                                                                                                                                                   |
| `TimestampType`    | ✅     |                                                                                                                                                                                   |
| `TimestampNTZType` | ✅     |                                                                                                                                                                                   |
| `TimeType`         | ⚠️     | Spark 4.1+. Native serialization is in place; some operators (sort, shuffle, min/max) are still being wired up ([#4288](https://github.com/apache/datafusion-comet/issues/4288)). |

## Interval

Interval type support is incremental and tracked by
[#4540](https://github.com/apache/datafusion-comet/issues/4540).

| Type                    | Status | Notes                                                                   |
| ----------------------- | ------ | ----------------------------------------------------------------------- |
| `YearMonthIntervalType` | ✅     | Supported for `make_ym_interval` and YearMonth interval multiplication. |
| `DayTimeIntervalType`   | 🔜     | Tracked by #4540.                                                       |
| `CalendarIntervalType`  | 🔜     | Tracked by #4540.                                                       |

## Complex

| Type         | Status | Notes                                                                                                                                                                                    |
| ------------ | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `StructType` | ✅     | Empty structs (no fields) fall back.                                                                                                                                                     |
| `ArrayType`  | ✅     |                                                                                                                                                                                          |
| `MapType`    | ✅     | Hash aggregate group keys cannot contain a `MapType` (transitively): Arrow's row format used by DataFusion's grouped hash aggregate does not support `Map`, so such groupings fall back. |

## Variant

| Type          | Status | Notes                                                                                                                                                                                                          |
| ------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `VariantType` | 🔜     | Spark 4.0+. Native scan support is tracked by [#4295](https://github.com/apache/datafusion-comet/issues/4295); shredded Parquet read/write by [#3983](https://github.com/apache/datafusion-comet/issues/3983). |

## Other

| Type       | Status | Notes |
| ---------- | ------ | ----- |
| `NullType` | ✅     |       |

## See also

- [Comet Compatibility Guide](compatibility/index.md) - known incompatibilities and edge cases.
- [Parquet Scan Compatibility](compatibility/scans.md) - per-type behavior at scan time.
- [Supported Spark Operators](operators.md) - the equivalent reference for operators.
- [Supported Spark Expressions](expressions.md) - the equivalent reference for expressions.
