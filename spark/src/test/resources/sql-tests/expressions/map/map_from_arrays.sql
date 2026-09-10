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

-- Spark's ArrayBasedMapBuilder rejects a NULL key element outright, ahead of the duplicate-key
-- check, and resolves duplicates by the default `spark.sql.mapKeyDedupPolicy` = `EXCEPTION`.
-- `map_from_arrays_dedup_policy.sql` covers `LAST_WIN`.

query expect_error(NULL_MAP_KEY)
SELECT map_from_arrays(array('a', NULL), array(1, 2))

-- a NULL key is reported as such even when it repeats, which a duplicate check would see first
query expect_error(NULL_MAP_KEY)
SELECT map_from_arrays(array(CAST(NULL AS STRING), NULL), array(1, 2))

query expect_error(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(array('a', 'a'), array(1, 2))

-- key and value arrays of different lengths. Spark reports this through a `_LEGACY_ERROR_TEMP_*`
-- condition whose number moves between Spark versions, so match on the message instead.
query expect_error(must have the same length)
SELECT map_from_arrays(array('a', 'b'), array(1))
