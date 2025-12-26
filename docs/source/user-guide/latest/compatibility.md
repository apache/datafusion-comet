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
This guide documents known differences and limitations.

---

## Parquet

Comet has the following limitations when reading Parquet files:

- Decimal values encoded using binary format are not supported.
- Default values for nested types (e.g., maps, arrays, structs) are not supported.
  Literal default values are supported.

---

## ANSI Mode

When ANSI mode is enabled, Comet falls back to Spark for some expressions to ensure correctness.

Affected expressions include:

- `Average`
- `Cast` (in some cases)

Incompatible expressions can be forced to run natively using:

```text
spark.comet.expression.EXPRNAME.allowIncompatible=true
```

This is not recommended for production use.

Tracking issue for full ANSI support:
https://github.com/apache/datafusion-comet/issues/313

---

## Floating-point Number Comparison

Spark normalizes NaN and signed zero in most cases, except during comparisons.
DataFusion (via Arrow) does not apply the same normalization.

Comet adds additional normalization logic but may still differ in rare edge cases,
especially when both positive and negative zero are present.

To force Spark execution for strict correctness:

```text
spark.comet.exec.strictFloatingPoint=true
```

---

## Incompatible Expressions

Expressions that are not fully Spark-compatible fall back to Spark by default.
They may be enabled using:

```text
spark.comet.expression.EXPRNAME.allowIncompatible=true
```

See the [Comet Supported Expressions Guide](expressions.md) for details.

---

## Regular Expressions

Comet uses Rust's regex crate, which differs from Java's regex engine used by Spark.
Patterns known to produce different results will fall back to Spark.

This behavior can be overridden using:

```text
spark.comet.regexp.allowIncompatible=true
```

---

## Window Functions

Window functions are disabled by default.
Support is incomplete and may produce incorrect results.

Tracking issue:
https://github.com/apache/datafusion-comet/issues/2721

---

## Cast

Cast operations are classified into three categories:

- **Compatible** – Results match Spark
- **Incompatible** – May differ for some inputs; fallback by default
- **Unsupported** – Always falls back to Spark

### Compatible Casts

The following casts are generally compatible with Spark, with noted differences.

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
| float | string | Precision differences possible |
| double | boolean |  |
| double | byte |  |
| double | short |  |
| double | integer |  |
| double | long |  |
| double | float |  |
| double | string | Precision differences possible |
| decimal | boolean |  |
| decimal | byte |  |
| decimal | short |  |
| decimal | integer |  |
| decimal | long |  |
| decimal | float |  |
| decimal | double |  |
| decimal | decimal |  |
| decimal | string | Formatting differences possible |
| string | boolean |  |
| string | byte |  |
| string | short |  |
| string | integer |  |
| string | long |  |
| string | float |  |
| string | double |  |
| string | binary |  |
| string | date | Years supported: 262143 BC to 262142 AD |
| binary | string |  |
| date | string |  |
| timestamp | long |  |
| timestamp | string |  |
| timestamp | date |  |
<!-- prettier-ignore-end -->
<!--END:COMPAT_CAST_TABLE-->

### Incompatible Casts

The following casts may produce different results and are disabled by default.

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->

<!--BEGIN:INCOMPAT_CAST_TABLE-->
<!-- prettier-ignore-start -->
| From Type | To Type | Notes |
|-|-|-|
| float | decimal | Rounding differences |
| double | decimal | Rounding differences |
| string | decimal | Fullwidth unicode digits and null bytes not supported |
| string | timestamp | Not all valid formats supported |
<!-- prettier-ignore-end -->
<!--END:INCOMPAT_CAST_TABLE-->

### Unsupported Casts

Any cast not listed above is currently unsupported and will fall back to Spark.

Tracking issue:
https://github.com/apache/datafusion-comet/issues/286

---

### Complex Type Casts

Comet provides native support for a limited set of complex type casts.
All other complex casts fall back to Spark.

#### Struct Type Casting

- **`STRUCT` → `STRING`**  
  Casting a struct to a string is supported.
  This includes:
  - Named structs
  - Nested structs
  - Structs containing primitive, decimal, date, and timestamp fields

  Example:

  ```sql
  WITH t AS (
    SELECT named_struct('a', 1, 'b', 'x') AS s
  )
  SELECT CAST(s AS STRING) FROM t;
  ```

- **`STRUCT` → `STRUCT`**  
  Casting between struct types is supported when the number of fields matches.
  Fields are matched by position, not by name, consistent with Spark behavior.

  Example:

  ```sql
  WITH t AS (
    SELECT named_struct('a', '1', 'b', '2') AS s
  )
  SELECT CAST(s AS struct<field1:string, field2:string>) FROM t;
  ```

#### Array Type Casting

- **`ARRAY<T>` → `STRING`**  
  Casting arrays to strings is supported and produces a string representation
  of the array contents.

  Supported element types include:
  - `boolean`
  - `byte`
  - `short`
  - `integer`
  - `long`
  - `string`
  - `decimal`

  Support depends on the scan implementation.
  Arrays with unsupported element types may fall back to Spark.

  Example:

  ```sql
  SELECT CAST(array(1, 2, 3) AS STRING);
  ```

#### Binary Type Casting

- **`BINARY` → `STRING`**  
  Casting binary values to strings is supported when the binary data is valid UTF-8.

- **`STRING` → `BINARY`**  
  Casting strings to binary values is supported.

  Example:

  ```sql
  SELECT CAST('abc' AS BINARY);
  SELECT CAST(binary('abc') AS STRING);
  ```

#### Limitations

The following complex casts are not supported natively and will fall back to Spark:

- `ARRAY` → `ARRAY`
- `STRUCT` → `ARRAY`
- `ARRAY` → `STRUCT`
- Any cast involving `MAP` types

Fallback behavior may depend on query planning and scan implementation.
