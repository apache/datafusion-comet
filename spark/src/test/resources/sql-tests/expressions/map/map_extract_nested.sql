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
CREATE TABLE test_map_extract_nested(m map<int,array<int>>, d map<int,array<array<int>>>, s map<int,struct<n:array<int>>>, mm map<int,map<int,array<int>>>, k int) USING parquet

-- Long unselected values expose over-reservation even with small batches. Equal middle
-- lengths in d and mm also exercise excess capacity in nested grandchildren.
statement
INSERT INTO test_map_extract_nested VALUES
  (map(1, array(), 2, sequence(1, 128)), map(1, array_repeat(array(), 8), 2, array_repeat(sequence(1, 128), 8)), map(1, named_struct('n', array()), 2, named_struct('n', sequence(1, 128))), map(1, map(1, array()), 2, map(1, sequence(1, 128))), 1),
  (map(1, array(10, NULL), 2, sequence(1, 128)), map(1, array(array(10), NULL), 2, array_repeat(sequence(1, 128), 8)), map(1, named_struct('n', array(10, NULL)), 2, named_struct('n', sequence(1, 128))), map(1, map(1, array(10), 2, NULL), 2, map(1, sequence(1, 128))), 1),
  (map(1, NULL, 2, sequence(1, 128)), map(1, NULL, 2, array_repeat(sequence(1, 128), 8)), map(1, NULL, 2, named_struct('n', sequence(1, 128))), map(1, NULL, 2, map(1, sequence(1, 128))), 1),
  (NULL, NULL, NULL, NULL, 1),
  (map(), map(), map(), map(), 1),
  (map(1, array(7)), map(1, array(array(7))), map(1, named_struct('n', array(7))), map(1, map(1, array(7))), NULL),
  (map(1, array(7)), map(1, array(array(7))), map(1, named_struct('n', array(7))), map(1, map(1, array(7))), 3),
  (map(1, array(7), 2, array(8, 9)), map(1, array(array(7)), 2, array(array(8, 9))), map(1, named_struct('n', array(7)), 2, named_struct('n', array(8, 9))), map(1, map(1, array(7)), 2, map(1, array(8, 9))), 2)

query expect_native(getmapvalue)
SELECT m[k], d[k], s[k], mm[k] FROM test_map_extract_nested

query expect_native(getmapvalue)
SELECT m[1], d[1], s[1], mm[1] FROM test_map_extract_nested

query expect_native(element_at)
SELECT element_at(m, k), element_at(d, k), element_at(s, k), element_at(mm, k) FROM test_map_extract_nested

query expect_native(element_at)
SELECT element_at(m, 1), element_at(d, 1), element_at(s, 1), element_at(mm, 1) FROM test_map_extract_nested

query expect_native(element_at)
SELECT element_at(m, 3), element_at(d, 3), element_at(s, 3), element_at(mm, 3) FROM test_map_extract_nested

query expect_native(element_at)
SELECT element_at(m, k), element_at(d, k), element_at(s, k), element_at(mm, k) FROM test_map_extract_nested WHERE k IS NULL
