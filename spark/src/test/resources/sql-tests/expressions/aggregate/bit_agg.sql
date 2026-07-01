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
CREATE TABLE test_bit_agg(i int, grp string) USING parquet

statement
INSERT INTO test_bit_agg VALUES (1, 'a'), (2, 'a'), (3, 'a'), (4, 'b'), (5, 'b'), (NULL, 'b')

query
SELECT bit_and(i), bit_or(i), bit_xor(i) FROM test_bit_agg

query
SELECT grp, bit_and(i), bit_or(i), bit_xor(i) FROM test_bit_agg GROUP BY grp ORDER BY grp

-- all integral types: tinyint, smallint, int, bigint
statement
CREATE TABLE test_bit_agg_types(b tinyint, s smallint, i int, l bigint, grp string) USING parquet

statement
INSERT INTO test_bit_agg_types VALUES
  (cast(1 as tinyint), cast(1 as smallint), 1, 1L, 'a'),
  (cast(3 as tinyint), cast(3 as smallint), 3, 3L, 'a'),
  (cast(7 as tinyint), cast(7 as smallint), 7, 7L, 'a'),
  (NULL, NULL, NULL, NULL, 'b'),
  (cast(-1 as tinyint), cast(-1 as smallint), -1, -1L, 'c'),
  (cast(0 as tinyint), cast(0 as smallint), 0, 0L, 'c')

-- exercise bit_and over each integral type
query
SELECT bit_and(b), bit_and(s), bit_and(i), bit_and(l) FROM test_bit_agg_types

-- group by including a group of all NULLs
query
SELECT grp, bit_and(b), bit_and(s), bit_and(i), bit_and(l)
FROM test_bit_agg_types GROUP BY grp ORDER BY grp

-- all-NULL filter, should return NULL
query
SELECT bit_and(i), bit_or(i), bit_xor(i) FROM test_bit_agg WHERE i IS NULL

-- empty input, should return NULL
query
SELECT bit_and(i), bit_or(i), bit_xor(i) FROM test_bit_agg WHERE 1 = 0

-- HAVING clause referencing bit_and
query
SELECT grp, bit_and(i) FROM test_bit_agg GROUP BY grp HAVING bit_and(i) < 7 ORDER BY grp

-- DISTINCT form
query
SELECT bit_and(DISTINCT i), bit_or(DISTINCT i), bit_xor(DISTINCT i) FROM test_bit_agg

-- boundary values: Long.MinValue, Long.MaxValue, Int.MinValue, Int.MaxValue
statement
CREATE TABLE test_bit_agg_bounds(i int, l bigint) USING parquet

statement
INSERT INTO test_bit_agg_bounds VALUES
  (-2147483648, -9223372036854775808L),
  (2147483647, 9223372036854775807L),
  (-1, -1L)

query
SELECT bit_and(i), bit_or(i), bit_xor(i), bit_and(l), bit_or(l), bit_xor(l) FROM test_bit_agg_bounds

-- literal argument
query
SELECT bit_and(5), bit_or(5), bit_xor(5) FROM test_bit_agg
