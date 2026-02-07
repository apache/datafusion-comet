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

-- ConfigMatrix: parquet.enable.dictionary=false,true

-- TODO: replace map_from_arrays with map whenever map is supported in Comet

-- Basic integer key tests with map literals
query
select map_contains_key(map_from_arrays(array(1, 2), array('a', 'b')), 5)

query
select map_contains_key(map_from_arrays(array(1, 2), array('a', 'b')), 1)

-- Decimal type coercion tests
-- TODO: requires map cast to be supported in Comet
query spark_answer_only
select map_contains_key(map_from_arrays(array(1, 2), array('a', 'b')), 5.0)

query spark_answer_only
select map_contains_key(map_from_arrays(array(1, 2), array('a', 'b')), 1.0)

query spark_answer_only
select map_contains_key(map_from_arrays(array(1.0, 2), array('a', 'b')), 5)

query spark_answer_only
select map_contains_key(map_from_arrays(array(1.0, 2), array('a', 'b')), 1)

-- Empty map tests
-- TODO: requires casting from NullType to be supported in Comet
query spark_answer_only
select map_contains_key(map_from_arrays(array(), array()), 0)

-- Test with table data
statement
CREATE TABLE test_map_contains_key(m map<string, int>) USING parquet

statement
INSERT INTO test_map_contains_key VALUES (map_from_arrays(array('a', 'b', 'c'), array(1, 2, 3))), (map_from_arrays(array('x'), array(10))), (map_from_arrays(array(), array())), (NULL)

query
SELECT map_contains_key(m, 'a') FROM test_map_contains_key

query
SELECT map_contains_key(m, 'x') FROM test_map_contains_key

query
SELECT map_contains_key(m, 'missing') FROM test_map_contains_key

-- Test with integer key map
statement
CREATE TABLE test_map_int_key(m map<int, string>) USING parquet

statement
INSERT INTO test_map_int_key VALUES (map_from_arrays(array(1, 2), array('a', 'b'))), (map_from_arrays(array(), array())), (NULL)

query
SELECT map_contains_key(m, 1) FROM test_map_int_key

query
SELECT map_contains_key(m, 5) FROM test_map_int_key
