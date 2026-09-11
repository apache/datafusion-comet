-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Spark 3.5-only regression for apache/datafusion-comet#5451 (review comment). Reproduces the
-- exact scenario from the PR review: a non-foldable Parquet column that is NULL as the first
-- argument to concat, followed by a sibling expression (`unbase64('A')`) that would raise
-- `Last unit does not have enough valid bits`. Spark 3.5's generated concat short-circuits on
-- the first NULL argument and never evaluates the sibling, so the whole projection returns
-- NULL. Before the fix on this branch, recursively serializing the compound child pushed
-- `unbase64(bad)` into a native ScalarFunctionExpr that evaluates arguments eagerly, turning
-- the previously-successful query into a task failure. The fix routes non-trivial children
-- through the JVM codegen dispatcher (CodegenDispatchFallback), restoring the whole-tree
-- dispatch behavior. This fixture is pinned to Spark 3.5 because Spark 4.x's generated concat
-- does not short-circuit the same way and its own reference execution raises on this input, so
-- the same query cannot be verified as returning NULL against a Spark 4.x reference.

-- MinSparkVersion: 3.5
-- MaxSparkVersion: 3.5

statement
CREATE TABLE test_unbase64_concat_short_circuit(n string, bad string) USING parquet

statement
INSERT INTO test_unbase64_concat_short_circuit VALUES (NULL, 'A'), ('YWJj', 'YWJj')

query
SELECT hex(unbase64(concat(n, cast(unbase64(bad) AS string))))
FROM test_unbase64_concat_short_circuit
