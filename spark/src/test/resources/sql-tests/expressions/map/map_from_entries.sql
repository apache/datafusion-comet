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
CREATE TABLE test_map_from_entries(entries array<struct<key:string, value:int>>) USING parquet

statement
INSERT INTO test_map_from_entries VALUES (array(struct('a', 1), struct('b', 2), struct('c', 3))), (array()), (NULL)

-- A NULL entry makes the whole map NULL, so no key of the row is inserted, not even a NULL one.
statement
INSERT INTO test_map_from_entries VALUES
  (array(CAST(NULL AS struct<key:string, value:int>), struct('b' AS key, 2 AS value))),
  (array(CAST(NULL AS struct<key:string, value:int>), struct(CAST(NULL AS STRING) AS key, 2 AS value)))

query
SELECT map_from_entries(entries) FROM test_map_from_entries

-- BinaryType key/value: the native map path is Incompatible, so the expression is routed
-- through the JVM codegen dispatcher and still executes natively with Spark-matching results.
query
SELECT map_from_entries(array(struct(cast('x' as binary), 10)))

query
SELECT map_from_entries(array(struct(10, cast('x' as binary))))

-- literal arguments
query spark_answer_only
SELECT map_from_entries(array(struct('x', 10), struct('y', 20), struct('z', 30)))

-- Spark's ArrayBasedMapBuilder rejects a NULL key and, under the default
-- `spark.sql.mapKeyDedupPolicy` = `EXCEPTION`, a duplicate key. It inserts a row's entries one at
-- a time, so whichever of the two comes first in the row is the one reported.
-- `map_from_entries_dedup_policy.sql` covers `LAST_WIN`.

query expect_error(NULL_MAP_KEY)
SELECT map_from_entries(array(struct(CAST(NULL AS STRING), 1), struct('b', 2)))

query expect_error(DUPLICATED_MAP_KEY)
SELECT map_from_entries(array(struct('a', 1), struct('a', 2)))

-- the duplicate comes first, so it is reported although a NULL key follows it
query expect_error(DUPLICATED_MAP_KEY)
SELECT map_from_entries(array(struct('a', 1), struct('a', 2), struct(CAST(NULL AS STRING), 3)))
