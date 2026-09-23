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

# Map Expressions

## MapSort (Spark 4.0+)

Spark 4.0 inserts `MapSort` to normalize map values when they appear in grouping expressions or
shuffle hash partitioning keys. Comet runs `MapSort` natively for supported scalar key types.

Other orderable key types have no native implementation. By default those cases fall the
enclosing projection or shuffle back to Spark. Set `spark.comet.expression.MapSort.codegen.enabled=true`
to run Spark's own generated JVM code through the codegen dispatcher instead, so the enclosing
operator can stay in the Comet pipeline. That dispatcher route is not a native `MapSort`
implementation, and a matched microbenchmark of these shapes is slower than the Spark fallback.

When `spark.comet.exec.strictFloatingPoint=true`, maps whose keys contain `Float` or `Double`
follow the same default: the enclosing operator falls back to Spark. The codegen setting above
routes those keys through the dispatcher instead (consistent with `SortOrder` and `SortArray`).
Arrow's sort uses IEEE total ordering for floating-point, which differs from Spark's
`Double.compare` semantics for `NaN` and `-0.0`. If the dispatcher is disabled or cannot handle
an expression, Comet safely falls back to Spark.

Set `spark.comet.expression.MapSort.enabled=false` to disable Comet `MapSort` entirely, including
the native scalar-key path. That expression-specific setting leaves the codegen dispatcher
available to unrelated expressions.

<!--BEGIN:EXPR_COMPAT[map]-->
<!--END:EXPR_COMPAT-->
