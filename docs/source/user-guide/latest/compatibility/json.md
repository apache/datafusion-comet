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

# JSON Compatibility

Comet supports two engines for evaluating JSON expressions (`get_json_object`,
`from_json`, `to_json`), selected by the `spark.comet.exec.json.engine`
configuration entry:

- `java` (default): routes evaluation through Comet's Arrow-direct codegen
  dispatcher so Spark's own `doGenCode` for the expression runs inside the Comet
  pipeline. Byte-exact compatibility with Spark, at the cost of a JNI roundtrip
  per batch. This rides the codegen dispatcher
  (`spark.comet.exec.scalaUDF.codegen.enabled`, enabled by default). If the
  dispatcher is disabled, the operator falls back to Spark.
- `rust`: native DataFusion implementation. Faster, but has known compatibility
  gaps with Spark on certain inputs. An expression or input case with no native
  implementation falls back to the `java` engine.

## Expression coverage

| SQL               | `rust` engine                                                                | `java` engine |
| ----------------- | ---------------------------------------------------------------------------- | ------------- |
| `get_json_object` | Supported, with gaps on single-quoted JSON and unescaped control characters  | Compatible    |
| `from_json`       | Supported with restrictions (PERMISSIVE mode only, simple schema types only) | Compatible    |
| `to_json`         | Supported for struct inputs only, no options                                 | Compatible    |

When the `rust` engine is selected but an expression or input case has no native
implementation (for example `to_json` with map or array inputs, or `from_json`
with an unsupported schema), Comet falls back to the `java` engine for that case.

## When to use the `rust` engine

- You want the faster native path and your inputs avoid the known compatibility
  gaps above.
- Enable it with `spark.comet.exec.json.engine=rust`. Cases the native path does
  not cover still fall back to the `java` engine.
