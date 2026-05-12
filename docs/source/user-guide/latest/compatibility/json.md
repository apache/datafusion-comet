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

Comet supports two engines for evaluating JSON expressions, selected by the
`spark.comet.exec.json.engine` configuration entry:

- `rust` (default): native DataFusion implementation. Fast, but has known
  compatibility gaps with Spark on certain inputs.
- `java` (experimental): routes evaluation through a JVM UDF that delegates to
  Spark's own expression classes for byte-exact compatibility, at the cost of a
  JNI roundtrip per batch.

## Expression coverage

| SQL               | `rust` engine                                                                | `java` engine |
| ----------------- | ---------------------------------------------------------------------------- | ------------- |
| `get_json_object` | Supported, with gaps on single-quoted JSON and unescaped control characters  | Compatible    |
| `from_json`       | Supported with restrictions (PERMISSIVE mode only, simple schema types only) | Compatible    |
| `to_json`         | Supported for struct inputs only, no options                                 | Compatible    |

## When to use the `java` engine

- You hit a compatibility gap in the `rust` engine and need exact Spark output.
- You can absorb the JNI overhead. Typically negligible relative to JSON parse
  cost, but verify with your own benchmarks.
