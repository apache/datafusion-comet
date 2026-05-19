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

# Scala UDF and Java UDF Support

Comet executes Spark's Scala and Java [scalar user-defined functions (UDFs)](https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html) on the native Comet path. The presence of a UDF does not force the enclosing operator off the native path; surrounding native operators stay native.

This page covers Spark's `ScalaUDF` (Scala `udf(...)`, `spark.udf.register(...)` over Scala or Java functional interfaces, and SQL `CREATE FUNCTION ... AS 'com.example.MyUDF'`). Other UDF kinds (Python / Pandas, Hive, aggregate) are out of scope and continue to fall back to Spark.

This feature is experimental and disabled by default.

## Configuration

| Key                                         | Default | Description                                                                                                        |
| ------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------ |
| `spark.comet.exec.scalaUDF.codegen.enabled` | `false` | When `true`, eligible `ScalaUDF`s run on the Comet path. When `false`, the enclosing operator falls back to Spark. |

## Supported

- User functions registered via `udf(...)`, `spark.udf.register(...)` (Scala or Java functional interfaces), or SQL `CREATE FUNCTION ... AS 'com.example.MyUDF'`.
- Scalar input/output types: `Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Decimal`, `String`, `Binary`, `Date`, `Timestamp`, `TimestampNTZ`.
- Complex input/output types with arbitrary nesting: `ArrayType`, `StructType`, `MapType`.
- Composition with other Catalyst expressions inside the argument tree (e.g. `myUdf(upper(s))` runs as one native unit).
- Higher-order functions (`transform`, `filter`, `exists`, `aggregate`, `zip_with`, `map_filter`, `map_zip_with`, etc.) inside the argument tree.

## Not supported

- Aggregate UDFs (`ScalaAggregator`, `TypedImperativeAggregate`, the legacy `UserDefinedAggregateFunction`).
- Table UDFs and generators.
- Python `@udf` and Pandas `@pandas_udf`.
- Hive `GenericUDF` and `SimpleUDF`.
- `CalendarIntervalType`, `NullType`, and `UserDefinedType` arguments and return types.
- Trees whose total nested-field count (output plus all input columns the UDF tree references) exceeds `spark.sql.codegen.maxFields` (default 100). Comet refuses these at plan time and the operator falls back to Spark.

When a UDF is rejected, the reason surfaces through Comet's standard fallback diagnostics; the query still runs on Spark.

### Working around UDT arguments

Spark `UserDefinedType`s (e.g. MLlib's `VectorUDT`) wrap an underlying SQL representation, typically a struct or array of supported scalar types. To run a UDF over a UDT-typed column on the Comet path, register the function over the underlying representation instead of the UDT class and reconstruct the UDT object inside the function body. Convert back to the underlying representation on output. The same pattern works for the return type: produce a struct / array of supported scalars instead of returning the UDT directly, and rehydrate at the call site if needed.

This is awkward but unblocks UDT use cases without losing native execution of the surrounding plan.

## Behavior

- Non-deterministic expressions referenced from the argument tree (`rand`, `uuid`, `monotonically_increasing_id`) produce per-partition sequences consistent with Spark.
- `TaskContext.get()` inside the user function returns the driving Spark task's context.
- The user function must be closure-serializable; the same function that works with Spark's executor execution works here.

## Known limitations

- Comet specializes the UDF once per query. Spark's analyzer produces a fresh `ScalaUDF` instance per query, so two structurally identical queries do not share a specialization. Within one query, batches of the same shape reuse the specialization.
