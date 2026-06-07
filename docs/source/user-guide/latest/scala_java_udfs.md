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

This feature is enabled by default. Set `spark.comet.exec.scalaUDF.codegen.enabled` to `false` to route plans containing a `ScalaUDF` back to Spark for the enclosing operator.

## Configuration

| Key                                         | Default | Description                                                                                                        |
| ------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------ |
| `spark.comet.exec.scalaUDF.codegen.enabled` | `true`  | When `true`, eligible `ScalaUDF`s run on the Comet path. When `false`, the enclosing operator falls back to Spark. |

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
- `CalendarIntervalType`, `NullType`, and `UserDefinedType` arguments and return types. UDT-typed columns fall back to Spark; for native execution, store and read the underlying representation directly (e.g. write MLlib `Vector` outputs as `Struct<type: Byte, size: Int, indices: Array<Int>, values: Array<Double>>` rather than `VectorUDT`).
- Trees whose total nested-field count (output plus all input columns the UDF tree references) exceeds `spark.sql.codegen.maxFields` (default 100). Comet refuses these at plan time and the operator falls back to Spark.

When a UDF is rejected, the reason surfaces through Comet's standard fallback diagnostics; the query still runs on Spark.

## Behavior

- Non-deterministic expressions referenced from the argument tree (`rand`, `uuid`, `monotonically_increasing_id`) produce per-partition sequences consistent with Spark.
- `TaskContext.get()` inside the user function returns the driving Spark task's context.
- The user function must be closure-serializable; the same function that works with Spark's executor execution works here.

## Known limitations

- Each query containing a ScalaUDF pays a one-time codegen cost on its first batch and reuses the compiled kernel for subsequent batches, matching Spark's whole-stage codegen behavior. Bytecode is deduped JVM-wide via the same `CodeGenerator` cache, so structurally identical queries across a session share the compiled class.
