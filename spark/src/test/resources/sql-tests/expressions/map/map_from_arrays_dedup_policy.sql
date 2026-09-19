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

-- Verifies that `map_from_arrays` routes through the JVM codegen dispatcher when
-- `spark.sql.mapKeyDedupPolicy` is set to `LAST_WIN`. Spark's ArrayBasedMapBuilder keeps the last
-- occurrence of each duplicate key; Comet's native `map` scalar has no LAST_WIN path, so the
-- dispatcher runs Spark's generated code inside the Comet pipeline. The default `EXCEPTION` mode
-- agrees with Comet's native implementation and is covered by `map_from_arrays.sql`.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Spark 3.4.3's ColumnarArray.copy drops primitive-array NULLs (SPARK-48019).
-- Use Spark's row reader as the reference; Comet still scans and dispatches this expression.
-- https://issues.apache.org/jira/browse/SPARK-48019
-- Config: spark.sql.parquet.enableNestedColumnVectorizedReader=false

statement
CREATE TABLE test_map_from_arrays_dedup(k array<string>, v array<int>) USING parquet

statement
INSERT INTO test_map_from_arrays_dedup VALUES
  (array('a', 'b', 'c'), array(1, 2, 3)),
  (array('a', 'a', 'b'), array(1, 2, 3)),
  (array('x', 'x'), array(10, 20)),
  -- NULL arrays stay as column inputs so NullPropagation cannot fold away map_from_arrays.
  (NULL, array(1)),
  (array('a'), NULL),
  (NULL, NULL),
  (cast(array() as array<string>), cast(array() as array<int>)),
  -- NULL values are allowed, including the last value of a duplicate key.
  (array('a', 'b'), array(NULL, 2)),
  (array('a', 'a', 'b'), array(1, NULL, 3))

-- literal duplicate keys under LAST_WIN: Spark's generated code keeps the last value.
query expect_dispatch(map_from_arrays)
SELECT map_from_arrays(array('a', 'a', 'b'), array(1, 2, 3))

-- column input dispatches the same way; the incompat branch is triggered by the SQLConf value,
-- not per-row content.
query expect_dispatch(map_from_arrays)
SELECT map_from_arrays(k, v) FROM test_map_from_arrays_dedup

-- Typed empty literal arrays exercise the dispatcher without introducing NullType inputs.
query expect_dispatch(map_from_arrays)
SELECT map_from_arrays(cast(array() as array<string>), cast(array() as array<int>))

statement
CREATE TABLE test_map_from_arrays_errors(id int, k array<string>, v array<int>) USING parquet

statement
INSERT INTO test_map_from_arrays_errors VALUES
  (0, array('a', 'a'), array(1, 2)),
  (1, array('a', NULL), array(1, 2)),
  (2, array('a', 'b'), array(1)),
  (3, array('a'), array(1, 2))

-- Positive sentinel for the error queries: identical column types and LAST_WIN configuration
-- must dispatch. CometMapExpressionSuite also pins dispatch and exception parity for each error.
query expect_dispatch(map_from_arrays)
SELECT map_from_arrays(k, v) FROM test_map_from_arrays_errors WHERE id = 0

-- LAST_WIN does not permit NULL keys; this tests Spark's builder, not the native #4680 path.
query expect_error(NULL_MAP_KEY)
SELECT map_from_arrays(k, v) FROM test_map_from_arrays_errors WHERE id = 1

-- Both directions of the array-length mismatch must fail.
query expect_error(The key array and value array of MapData must have the same length)
SELECT map_from_arrays(k, v) FROM test_map_from_arrays_errors WHERE id = 2

query expect_error(The key array and value array of MapData must have the same length)
SELECT map_from_arrays(k, v) FROM test_map_from_arrays_errors WHERE id = 3
