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

# json_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## from_json

- Partial native support, marked `Incompatible` (requires explicit schema).

## get_json_object

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `BinaryExpression with ExpectsInputTypes with CodegenFallback`; `inputTypes = Seq(StringType, StringType) -> StringType`. Eval is inline and uses Jackson with `RawStyle` output. Foldable paths are parsed once. Returns NULL for invalid JSON, missing paths, or `JsonProcessingException`.
- Spark 4.0.1 (audited 2026-05-27): the eval is extracted into a `GetJsonObjectEvaluator` helper (no behaviour change). The trait set now mixes in `DefaultStringProducingExpression`, and `inputTypes` is widened to `StringTypeWithCollation(supportsTrimCollation = true)` for both arguments.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known incompatibility: Spark accepts single-quoted JSON and unescaped control characters; Comet's native parser (built on `serde_json`) rejects both, so those inputs require `spark.comet.expression.GetJsonObject.allowIncompatible=true` and may still produce different results. Non-default Spark 4.0 string collations are not propagated (https://github.com/apache/datafusion-comet/issues/2190).

## to_json

- Partial native support; options and map/array inputs fall back.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
