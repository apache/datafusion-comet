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

-- The native NULL guard serializes both children twice, so a nondeterministic child would drift
-- between the copies and falls back to Spark instead.

statement
CREATE TABLE test_map_from_arrays_nondet(_1 int, k array<int>, v array<int>) USING parquet

statement
INSERT INTO test_map_from_arrays_nondet
SELECT id, IF(id % 4 = 3, NULL, array(id, id + 100)), array(id * 2, id * 2 + 1) FROM range(0, 16)

query expect_fallback(because the native `NULL` guard would evaluate it twice)
SELECT _1, map_from_arrays(IF(monotonically_increasing_id() % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), array(2)) AS m
FROM test_map_from_arrays_nondet

query expect_fallback(because the native `NULL` guard would evaluate it twice)
SELECT _1, map_from_arrays(array(1), IF(monotonically_increasing_id() % 2 = 0, array(2), CAST(NULL AS ARRAY<INT>))) AS m
FROM test_map_from_arrays_nondet

-- Declined even when non-nullable, rather than relying on the guard matching every row
query expect_fallback(because the native `NULL` guard would evaluate it twice)
SELECT _1, map_from_arrays(array(monotonically_increasing_id()), array(2)) AS m
FROM test_map_from_arrays_nondet

-- Every fourth row brings a NULL keys array from the input rather than from the IF
query expect_fallback(because the native `NULL` guard would evaluate it twice)
SELECT _1, map_from_arrays(IF(monotonically_increasing_id() % 2 = 0, k, CAST(NULL AS ARRAY<INT>)), v) AS m
FROM test_map_from_arrays_nondet

query expect_native(map_from_arrays)
SELECT _1, map_from_arrays(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), array(2)) AS m
FROM test_map_from_arrays_nondet

-- DOUBLE keys are Incompatible, so by default the codegen dispatcher runs Spark's own code
query expect_dispatch(map_from_arrays)
SELECT _1, map_from_arrays(IF(monotonically_increasing_id() % 2 = 0, array(CAST(_1 AS DOUBLE)), CAST(NULL AS ARRAY<DOUBLE>)), array(2)) AS m
FROM test_map_from_arrays_nondet

statement
SET spark.comet.expression.MapFromArrays.allowIncompatible=true

-- The native opt-in reaches convert directly, which still declines the nondeterministic child
query expect_fallback(because the native `NULL` guard would evaluate it twice)
SELECT _1, map_from_arrays(IF(monotonically_increasing_id() % 2 = 0, array(CAST(_1 AS DOUBLE)), CAST(NULL AS ARRAY<DOUBLE>)), array(2)) AS m
FROM test_map_from_arrays_nondet
