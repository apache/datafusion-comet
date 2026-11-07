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
CREATE TABLE test_get_array_item(arr array<int>, idx int) USING parquet

statement
INSERT INTO test_get_array_item VALUES (array(10, 20, 30), 0), (array(10, 20, 30), 1), (array(10, 20, 30), 2), (array(1), 0), (NULL, 0), (array(10, 20), NULL)

query
SELECT arr[0], arr[1], arr[2] FROM test_get_array_item

query
SELECT arr[idx] FROM test_get_array_item

-- literal arguments
query
SELECT array(10, 20, 30)[0], array(10, 20, 30)[2], array()[0]

statement
CREATE TABLE test_get_array_item_complex(nested array<array<int>>, structs array<struct<a: int, b: string>>, idx int) USING parquet

statement
INSERT INTO test_get_array_item_complex VALUES
  (array(array(1, 2), array(3, 4)), array(named_struct('a', 1, 'b', 'a'), named_struct('a', 2, 'b', 'b')), 1),
  (NULL, NULL, 0),
  (array(array(5, 6)), array(named_struct('a', 3, 'b', 'c')), NULL)

query
SELECT nested[idx] FROM test_get_array_item_complex

query
SELECT structs[idx] FROM test_get_array_item_complex

-- Non-INT ordinals: Spark keeps the low 32 bits of a BIGINT
statement
CREATE TABLE test_get_array_item_ordinals(arr array<int>, b tinyint, s smallint, l bigint) USING parquet

statement
INSERT INTO test_get_array_item_ordinals VALUES
  (array(10, 20, 30), 0, 1, 2),
  (array(10, 20, 30), 2, 0, 1),
  (array(10, 20, 30), 1, 2, 4294967296),
  (array(10, 20, 30), 0, 1, 4294967298),
  (array(10, 20, 30), 0, 0, -4294967296),
  (array(10, 20, 30), 0, 0, -4294967294),
  (array(10, 20, 30), -1, -1, 2147483648),
  (array(10, 20, 30), 3, 3, 4294967295),
  (array(10, 20, 30), 0, 0, 9223372036854775807),
  (array(10, 20, 30), 0, 0, -9223372036854775808),
  (NULL, 0, 0, 0),
  (array(10, 20), NULL, NULL, NULL)

query
SELECT arr[b], arr[s], arr[l] FROM test_get_array_item_ordinals

query
SELECT arr[CAST(1 AS BIGINT)], arr[l - 1], arr[floor(l / 2)] FROM test_get_array_item_ordinals WHERE l BETWEEN 0 AND 2

-- A median: floor(DOUBLE) returns a BIGINT ordinal
query
SELECT g, sort_array(collect_list(v))[floor(count(*) / 2)] FROM
  (SELECT CAST(id % 3 AS INT) g, CAST(id AS INT) v FROM range(10)) GROUP BY g ORDER BY g
