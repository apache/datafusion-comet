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
CREATE TABLE test_flatten(arr array<array<int>>) USING parquet

statement
INSERT INTO test_flatten VALUES
  (array(array(1, 2), array(3, 4))),
  (array(array(), array(1))),
  (array()),
  (NULL),
  (array(array(1, NULL), array(NULL))),
  (array(array(1), CAST(NULL AS ARRAY<INT>))),
  (array(CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>)))

query
SELECT flatten(arr) FROM test_flatten

-- literal arguments
query
SELECT flatten(array(array(1, 2), array(3, 4)))

-- binary elements
statement
CREATE TABLE test_flatten_binary(arr array<array<binary>>) USING parquet

statement
INSERT INTO test_flatten_binary VALUES
  (array(array(X'01', X'0203'), array(X'', X'04'))),
  (array(array(), array(X'05'))),
  (array()),
  (NULL),
  (array(array(X'06', NULL), array(NULL))),
  (array(array(X'07'), CAST(NULL AS ARRAY<BINARY>))),
  (array(CAST(NULL AS ARRAY<BINARY>), CAST(NULL AS ARRAY<BINARY>)))

query
SELECT flatten(arr) FROM test_flatten_binary

-- flatten of binary elements as an aggregate input
query
SELECT count(flatten(arr)), count(DISTINCT flatten(arr)) FROM test_flatten_binary

-- nested flatten of binary elements, filtered on an element and used under a distinct aggregate
statement
CREATE TABLE test_flatten_binary3(k string, id int, arr array<array<array<binary>>>) USING parquet

statement
INSERT INTO test_flatten_binary3 VALUES
  ('a', 1, array(array(array(X'30313233343536373839414243', X'01')), array(array(X'02')))),
  ('a', 2, array(array(array(X'30313233343536373839414243')), array(array(X'03')))),
  ('a', 2, array(array(array(X'3031')), array(array(X'03')))),
  ('b', 3, array(array(array(X'30313233343536373839414243')))),
  ('b', 4, array(array(array(CAST(NULL AS BINARY))))),
  ('b', 5, NULL)

query
SELECT k, COUNT(DISTINCT id) FROM test_flatten_binary3
WHERE length(flatten(flatten(arr))[0]) = 13 GROUP BY k

query
SELECT COUNT(1) FROM test_flatten_binary3 WHERE length(flatten(flatten(arr))[0]) != 13

query
SELECT k, COUNT(1) FROM test_flatten_binary3 WHERE length(flatten(flatten(arr))[0]) = 13 GROUP BY 1
