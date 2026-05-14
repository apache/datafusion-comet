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

# ScalaUDF codegen dispatch

Comet can route Spark `ScalaUDF` expressions through a JVM-side kernel that processes Arrow batches directly, instead of falling back to Spark for the whole operator. The kernel is compiled per `(expression, input schema)` pair via Janino and reused across batches of the same query. Surrounding native operators stay on the Comet path. The cost is one JNI roundtrip per batch.

## Configuration

| Key                                         | Default | Description                                                                                                                                       |
| ------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.comet.exec.scalaUDF.codegen.enabled` | `true`  | When `true`, eligible `ScalaUDF`s route through the dispatcher. When `false`, plans containing a `ScalaUDF` fall back to Spark for that operator. |

## Supported

- User functions registered via `udf(...)`, `spark.udf.register(...)` (Scala or Java functional interfaces), or SQL `CREATE FUNCTION ... AS 'com.example.MyUDF'`.
- Scalar input and output types: `Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Decimal`, `String`, `Binary`, `Date`, `Timestamp`, `TimestampNTZ`.
- Complex input and output types with arbitrary nesting: `ArrayType`, `StructType`, `MapType`.
- Composition with other Catalyst expressions inside the user function's argument tree (e.g. `myUdf(upper(s))` binds the whole tree and compiles into one kernel).

## Not supported

- Aggregate UDFs (`ScalaAggregator`, `TypedImperativeAggregate`, the legacy `UserDefinedAggregateFunction`).
- Table UDFs and generators.
- Python `@udf` and Pandas `@pandas_udf`.
- Hive `GenericUDF` and `SimpleUDF`.
- `CalendarIntervalType` arguments and return types.

## Behavior

- Non-deterministic expressions referenced from the UDF's argument tree (`rand`, `uuid`, `monotonically_increasing_id`) produce per-partition sequences consistent with Spark. The kernel instance lives for one Spark task; state resets at task boundaries.
- `TaskContext.get()` inside the user function returns the driving Spark task's context even though the kernel runs on a Tokio worker thread.
- The user function must be closure-serializable. The same function that works with Spark's executor execution works here.

## Known limitations

- Each query analysis recompiles the kernel once. Spark's analyzer produces a fresh `ScalaUDF` instance per query, and the encoders embedded in that instance carry attribute references with fresh ids that the cache key cannot canonicalize across queries. Within one query, multiple batches of the same shape reuse the compiled kernel.
