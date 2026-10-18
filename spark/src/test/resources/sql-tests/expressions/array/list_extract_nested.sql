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


-- Config: spark.sql.ansi.enabled=false
-- Config: spark.comet.batchSize=3

statement
CREATE TABLE test_list_extract_nested(a array<array<int>>, m array<map<int,int>>, s array<struct<n:array<int>>>, idx int) USING parquet

-- Long unselected children force compaction when the empty first element is selected.
statement
INSERT INTO test_list_extract_nested VALUES
  (array(array(), sequence(1, 64)), array(map(), map_from_arrays(sequence(1, 64), sequence(1, 64))), array(named_struct('n', array()), named_struct('n', sequence(1, 64))), 0),
  (array(array(), array(1, NULL, 3)), array(map(), map(1, 2)), array(named_struct('n', array()), named_struct('n', array(1, NULL, 3))), 0),
  (array(array(10), array(20, 21)), array(map(10, 11), map(20, 21)), array(named_struct('n', array(10)), named_struct('n', array(20, 21))), 1),
  (array(NULL, array(1)), array(NULL, map(1, 2)), array(NULL, named_struct('n', array(1))), 0),
  (NULL, NULL, NULL, 0),
  (array(array(), array(2)), array(map(), map(2, 3)), array(named_struct('n', array()), named_struct('n', array(2))), NULL),
  (array(array(1)), array(map(1, 2)), array(named_struct('n', array(1))), 5),
  (array(array(1)), array(map(1, 2)), array(named_struct('n', array(1))), -1)

-- Column ordinals, selected NULL children, NULL lists and ordinals, and invalid indices.
query expect_native(getarrayitem)
SELECT a[idx], m[idx], s[idx] FROM test_list_extract_nested

-- Literal ordinals select heterogeneous empty and short children.
query expect_native(getarrayitem)
SELECT a[0], m[0], s[0] FROM test_list_extract_nested

-- One-based indexing and negative indexing share the native ListExtract kernel.
query expect_native(element_at)
SELECT element_at(a, 1), element_at(m, 1), element_at(s, 1) FROM test_list_extract_nested

query expect_native(element_at)
SELECT element_at(a, -1), element_at(m, -1), element_at(s, -1) FROM test_list_extract_nested

query expect_native(element_at)
SELECT element_at(a, idx + 1), element_at(m, idx + 1), element_at(s, idx + 1) FROM test_list_extract_nested WHERE idx >= 0 OR idx IS NULL


-- Equal middle lengths can still propagate oversized reservations into deeper children.
statement
CREATE TABLE test_list_extract_deep(a array<array<array<int>>>, m array<map<int,array<int>>>, idx int) USING parquet

-- Equal middle lengths with long unselected descendants exercise recursive compaction.
statement
INSERT INTO test_list_extract_deep VALUES
  (array(array_repeat(array(), 32), array_repeat(sequence(1, 4), 32)), array(map_from_arrays(sequence(1, 32), array_repeat(array(), 32)), map_from_arrays(sequence(1, 32), array_repeat(sequence(1, 4), 32))), 0),
  (array(array(array(), array()), array(array(1, 2), array(3, 4))), array(map(1, array(), 2, array()), map(1, array(1, 2), 2, array(3, 4))), 0),
  (array(array(array(10), array(20)), array(array(30, 31), array(40, 41))), array(map(1, array(10), 2, array(20)), map(1, array(30, 31), 2, array(40, 41))), 1),
  (array(array(NULL, array()), array(array(1))), array(map(1, NULL, 2, array()), map(1, array(1))), 0),
  (NULL, NULL, 0),
  (array(array(array(1))), array(map(1, array(1))), NULL)

query expect_native(getarrayitem)
SELECT a[idx], m[idx] FROM test_list_extract_deep

query expect_native(element_at)
SELECT element_at(a, 1), element_at(m, 1) FROM test_list_extract_deep

query expect_native(element_at)
SELECT element_at(a, -1), element_at(m, -1) FROM test_list_extract_deep
