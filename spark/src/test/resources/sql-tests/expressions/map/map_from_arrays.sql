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

statement
CREATE TABLE test_map_from_arrays(k array<string>, v array<int>) USING parquet

statement
INSERT INTO test_map_from_arrays VALUES
  (array('a', 'b', 'c'), array(1, 2, 3)),
  (array(), array()),
  (NULL, NULL),
  (array('x'), NULL),
  (NULL, array(99))

-- basic functionality
query spark_answer_only
SELECT map_from_arrays(k, v) FROM test_map_from_arrays WHERE k IS NOT NULL AND v IS NOT NULL

-- both inputs NULL should return NULL
query
SELECT map_from_arrays(k, v) FROM test_map_from_arrays WHERE k IS NULL AND v IS NULL

-- keys not null but values null should return NULL (Spark behavior)
query
SELECT map_from_arrays(k, v) FROM test_map_from_arrays WHERE k IS NOT NULL AND v IS NULL

-- keys null but values not null should return NULL (Spark behavior)
query
SELECT map_from_arrays(k, v) FROM test_map_from_arrays WHERE k IS NULL AND v IS NOT NULL

-- all rows including nulls
query spark_answer_only
SELECT map_from_arrays(k, v) FROM test_map_from_arrays

-- literal arguments
query spark_answer_only
SELECT map_from_arrays(array('a', 'b'), array(1, 2))

-- literal null arguments
query
SELECT map_from_arrays(NULL, array(1, 2))

query
SELECT map_from_arrays(array('a'), NULL)

query
SELECT map_from_arrays(NULL, NULL)

-- empty arrays produce MapType(NullType, NullType)
query
SELECT map_from_arrays(array(), array())

-- The serde's null guard serializes both inputs twice, and a non-deterministic input would
-- advance differently in each copy, so it falls back.
query expect_fallback(non-deterministic child under a null guard is evaluated on different rows than Spark's)
SELECT map_from_arrays(IF(monotonically_increasing_id() % 2 = 0, k, NULL), v) FROM test_map_from_arrays

-- A literal array beside a per-row one: DataFusion's map kernel reads the scalar list through its
-- first row only and fails the length check, so the shape stays in Spark. Independent of NullType
-- (the typed literal fails the same way); found while probing the NullType flavour.
query expect_fallback(native map takes the first row of a scalar list where the other argument is per-row)
SELECT map_from_arrays(array(coalesce(k[0], 'x')), array(1)), map_from_arrays(array(coalesce(k[0], 'x')), array(NULL)) FROM test_map_from_arrays
