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

# Reducing Row/Columnar Conversion Overhead

When a query stage contains many operators that fall back to Spark row-based execution, Comet may insert
repeated columnar-to-row and row-to-columnar conversions that dominate stage runtime. Set
`spark.comet.exec.transitionRevert.enabled=true` to have Comet revert the entire stage to Spark row execution
when the number of columnar-to-row transitions exceeds
`spark.comet.exec.transitionRevert.maxTransitions` (default `2`). This trades native execution of a small
subset of operators for eliminating conversion overhead across the stage. A stage is not reverted when it holds a
native aggregate whose intermediate buffer Spark cannot exchange with Comet across a stage boundary, because
reverting it would split that aggregate between the two engines.

## Wide or Deeply Nested Schemas

The cost of each conversion also grows sharply with schema shape: for wide or deeply nested schemas,
columnar-to-row conversion is especially expensive because the conversion work scales with the number of
columns and nested fields. If profiling shows these conversions dominating a query over such a schema, set
`spark.comet.exec.transitionRevert.enabled=true` and lower
`spark.comet.exec.transitionRevert.maxTransitions` (default `2`) to `1`. Note that this reverts every stage that
exceeds the threshold to Spark row-based execution — Comet removes the stage's native operators rather than running a
mix of native and fallback operators joined by repeated conversions — which can be cheaper than paying the
expensive conversions again and again.
