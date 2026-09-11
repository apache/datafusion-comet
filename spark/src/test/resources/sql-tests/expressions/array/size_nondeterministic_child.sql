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

-- `CometSize` reproduces Spark's NULL propagation with a `CASE WHEN child IS NOT NULL` guard
-- that serializes the child twice. A stateful child advances each copy independently, so the
-- guard and the `size` see different rows and the result silently drifts from Spark. The serde
-- declines any nondeterministic child and routes it through the JVM codegen dispatcher, which
-- evaluates the child once. A deterministic nullable child keeps the native guard.

-- ConfigMatrix: spark.sql.legacy.sizeOfNull=true,false

statement
CREATE TABLE test_size_nondet(_1 int) USING parquet

statement
INSERT INTO test_size_nondet VALUES
  (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15)

-- Spark returns 1 on every row whose array is non-NULL and the sentinel on the rest.
query expect_dispatch(size)
SELECT _1, size(IF(monotonically_increasing_id() % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>))) AS s
FROM test_size_nondet

-- A non-nullable stateful child is declined too, rather than relying on the guard matching
-- every row.
query expect_dispatch(size)
SELECT _1, size(array(monotonically_increasing_id())) AS s
FROM test_size_nondet

-- A deterministic nullable child stays on the native guarded path.
query expect_native(size)
SELECT _1, size(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>))) AS s
FROM test_size_nondet
