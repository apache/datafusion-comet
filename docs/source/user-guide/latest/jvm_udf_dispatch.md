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

# JVM UDF dispatch

Comet can route scalar expressions that lack a native DataFusion implementation, or whose native implementation diverges from Spark, through a JVM-side kernel that processes Arrow batches directly. Surrounding native operators stay on the Comet path instead of forcing a whole-plan fallback to Spark. The tradeoff is a JNI roundtrip and per-batch JVM execution.

## Supported expressions

- User-defined scalar functions registered via `spark.udf.register` (Scala `UDF1`/`UDF2`/... or Java functional interfaces), `udf(...)`, or SQL `CREATE FUNCTION ... AS 'com.example.MyUDF'`.
- Regex family: `rlike`, `regexp_replace`, `regexp_extract`, `regexp_extract_all`, `regexp_instr`, and `split` with a literal regex pattern.

Not supported:

- Aggregate UDFs, table UDFs, generators.
- Python `@udf` and Pandas `@pandas_udf`.
- Hive `GenericUDF` / `SimpleUDF`.

## Supported types

Scalar: `Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Decimal`, `String`, `Binary`, `Date`, `Timestamp`, `TimestampNTZ`.

Complex (as both input and output, including arbitrary nesting): `ArrayType`, `StructType`, `MapType`.

## Configuration

| Key                                     | Default | Description                                                                                                                                                                            |
| --------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.comet.exec.codegenDispatch.mode` | `auto`  | `auto` routes through JVM codegen when it is the serde's primary path (regex with java engine, ScalaUDF). `force` routes through codegen whenever accepted. `disabled` never routes through codegen. |
| `spark.comet.exec.regexp.engine`        | `java`  | `java` uses the JVM codegen path for the regex family. `rust` prefers the native DataFusion engine where one exists and falls back to Spark otherwise.                                 |

## Regex routing

Cells name the path the expression takes. `Spark` means the plan falls back to Spark. `codegen` means the JVM codegen dispatcher. `native` means the DataFusion scalar function.

| Expression           | `java, auto` | `java, force` | `java, disabled` | `rust, auto` | `rust, force` | `rust, disabled` |
| -------------------- | ------------ | ------------- | ---------------- | ------------ | ------------- | ---------------- |
| `rlike`              | codegen      | codegen       | Spark            | native       | codegen       | native           |
| `regexp_replace`     | codegen      | codegen       | Spark            | native       | codegen       | native           |
| `regexp_extract`     | codegen      | codegen       | Spark            | Spark        | Spark         | Spark            |
| `regexp_extract_all` | codegen      | codegen       | Spark            | Spark        | Spark         | Spark            |
| `regexp_instr`       | codegen      | codegen       | Spark            | Spark        | Spark         | Spark            |
| `split`              | codegen      | codegen       | Spark            | native       | codegen       | native           |

`regexp_extract`, `regexp_extract_all`, and `regexp_instr` have no native DataFusion path, so rust-engine cells read `Spark` regardless of dispatch mode. Rust-engine cells also require `spark.comet.expr.allow.incompat=true` for patterns the rust engine evaluates incompatibly with Spark; otherwise the plan falls back to Spark.

## Behavior notes

- Non-deterministic expressions (`rand`, `uuid`, `monotonically_increasing_id`) produce per-partition sequences consistent with Spark. One kernel instance lives per partition; state is reset on partition boundaries.
- ScalaUDF bodies that read `TaskContext.get()` see the correct partition context even when executed on a Tokio worker thread.
- The user function must be closure-serializable. The same function that works with Spark's executor execution works here.

## Known limitations

- Dictionary-encoded inputs are not handled. Comet's native scan and shuffle paths materialize dictionaries before the dispatcher, so this is not a current failure mode. If you observe it, file an issue.
- `regexp_replace` on a collated subject rejects at plan time; Spark wraps the pattern in `Collate(Literal, ...)` and the serde requires a bare `Literal`.
- `rlike` on ICU collations (e.g. `UNICODE_CI`) is a type mismatch in Spark itself, not a Comet-specific limitation. Binary collations like `UTF8_LCASE` work.

For internals (architecture, caching, compile-time specializations, open work items), see the contributor guide [JVM UDF Dispatch](../../contributor-guide/jvm_udf_dispatch.md) page.
