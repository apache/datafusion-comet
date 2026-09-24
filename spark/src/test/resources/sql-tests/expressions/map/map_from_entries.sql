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

-- an array holding a NULL entry is NULL before its keys are checked for NULL or a repeat
statement
CREATE TABLE test_map_from_entries_keys(
  id int,
  i array<struct<key: int, value: string>>,
  d array<struct<key: double, value: string>>,
  s array<struct<key: string, value: string>>) USING parquet

statement
INSERT INTO test_map_from_entries_keys VALUES
  (1,
   array(struct(-2147483648, 'x'), struct(2147483647, NULL), struct(0, 'z')),
   array(struct(double('NaN'), 'x'), struct(double('Infinity'), NULL),
     struct(double('-Infinity'), 'z'), struct(double('0.0'), 'w')),
   array(struct('', 'x'), struct('é', NULL), struct('中文', 'z'))),
  (2, array(), array(), array()),
  (3, NULL, NULL, NULL),
  (4, array(struct(1, 'a'), NULL, struct(1, 'b'), struct(NULL, 'c')), array(NULL),
   array(struct('a', 'x'), NULL)),
  (5, array(struct(1, 'a'), struct(2, 'b'), struct(1, 'c'), struct(NULL, 'd')), NULL, NULL),
  (6, array(struct(NULL, 'a'), struct(1, 'b'), struct(1, 'c')), NULL, NULL)

-- `d` leaves out `-0.0`, which Spark 4.0+ returns as `0.0` (see floating-point.md#map-keys)
query
SELECT id, map_from_entries(i), map_from_entries(d), map_from_entries(s)
FROM test_map_from_entries_keys WHERE id <= 4

-- the repeat comes before the NULL key. Matched on the hint only Spark's message carries, as
-- datafusion-spark's `map_from_entries` message also names `DUPLICATED_MAP_KEY`.
query expect_error(you can set "spark.sql.mapKeyDedupPolicy" to "LAST_WIN")
SELECT map_from_entries(i) FROM test_map_from_entries_keys WHERE id = 5

-- the NULL key comes before the repeat
query expect_error(NULL_MAP_KEY)
SELECT map_from_entries(i) FROM test_map_from_entries_keys WHERE id = 6
