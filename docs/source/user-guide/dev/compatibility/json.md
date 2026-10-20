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

Comet can evaluate JSON expressions (`get_json_object`, `from_json`, `to_json`,
`json_array_length`) two ways:

- **Codegen dispatcher (default):** Spark's own `doGenCode` for the expression
  runs inside the Comet pipeline (via Comet's Arrow-direct codegen dispatcher),
  giving byte-exact compatibility with Spark at the cost of a JNI roundtrip per
  batch. This rides the codegen dispatcher
  (`spark.comet.exec.scalaUDF.codegen.enabled`, enabled by default); if the
  dispatcher is disabled, the operator falls back to Spark.
- **Native (rust) path:** the native DataFusion implementation. Faster, but has
  known compatibility gaps with Spark on certain inputs, so it is **opt-in per
  expression** via the expression's `allowIncompatible` config. Any expression or
  input case with no native implementation falls back to the codegen dispatcher.

## Expression coverage

| SQL                 | Native (rust) path                                                                             | Opt-in config                                                |
| ------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| `get_json_object`   | Supported, with gaps on single-quoted JSON and unescaped control characters                    | `spark.comet.expression.GetJsonObject.allowIncompatible`     |
| `from_json`         | Supported with restrictions (PERMISSIVE mode only, simple schema types only)                   | `spark.comet.expression.JsonToStructs.allowIncompatible`     |
| `to_json`           | Supported for struct inputs only, no options                                                   | `spark.comet.expression.StructsToJson.allowIncompatible`     |
| `json_array_length` | Supported, with gaps on single-quoted JSON, unescaped control characters, and trailing content | `spark.comet.expression.LengthOfJsonArray.allowIncompatible` |

When the native path is enabled but an expression or input case has no native
implementation (for example `to_json` with map or array inputs, or `from_json`
with an unsupported schema), Comet falls back to the codegen dispatcher for that
case.

## When to use the native path

- You want the faster native path and your inputs avoid the known compatibility
  gaps above.
- Enable it per expression, for example
  `spark.comet.expression.GetJsonObject.allowIncompatible=true`. Cases the native path
  does not cover still fall back to the codegen dispatcher.
