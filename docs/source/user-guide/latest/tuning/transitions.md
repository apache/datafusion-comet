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

## Cost-Based Stage Fallback (Experimental)

Counting transitions does not say how much work each transition does. One transition that converts every row of a
large scan can cost more than the native scan saves, while one that converts a handful of aggregated rows costs
almost nothing. Set `spark.comet.exec.costModel.enabled=true` to have Comet estimate, for each query stage, the
cost of running the stage with Comet and the cost of running it with Spark, and revert the stage to Spark when
the estimated speedup (Spark cost divided by Comet cost) is below `spark.comet.exec.costModel.minSpeedup`
(default `1.0`).

The default cost model charges each operator for the number of rows it is estimated to process:

- A Spark operator costs its input rows times a weight for the kind of operator. Scans and transitions also
  scale with the width of the schema.
- A Comet native operator costs the Spark operator's cost divided by `spark.comet.exec.costModel.nativeSpeedup`
  (default `2.0`).
- A conversion between Comet's columnar batches and Spark rows costs
  `spark.comet.exec.costModel.transitionCostFactor` (default `3.0`) times what Spark pays to convert the output
  of its own vectorized scan to rows.

Row counts come from the runtime statistics of completed shuffle stages when adaptive query execution is
enabled, and otherwise from the size of the scanned tables, with fixed selectivities for filters, aggregates and
joins.

For example, when a native scan is followed immediately by a projection that falls back to Spark, every scanned
row has to be converted, and with the default settings the stage is reverted. If a native filter or aggregate
runs before the fallback, far fewer rows are converted, and the stage keeps running with Comet. A reverted stage
reports the estimated speedup as its fallback reason in the explain output.

The default weights are initial estimates that have not been calibrated against benchmarks, so treat the feature
as a tool for experimentation. To use a different model, implement `org.apache.comet.cost.CometCostModel` and set
`spark.comet.exec.costModel.class` to the name of the class. The model is given two plans for each stage, the
stage as Comet would run it and the same stage reverted to Spark, and returns a cost for each.

The cost model and `spark.comet.exec.transitionRevert.enabled` are independent: when both are enabled, a stage is
reverted if either one asks for it.
