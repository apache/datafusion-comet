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

-- Run with and without Parquet dictionary encoding, which changes how string and binary columns
-- are read.
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_min_max(i int, d double, s string, grp string) USING parquet

statement
INSERT INTO test_min_max VALUES (1, 1.5, 'b', 'x'), (3, 3.5, 'a', 'x'), (2, 2.5, 'c', 'y'), (NULL, NULL, NULL, 'y'), (-1, -1.5, 'z', 'x')

-- Spark plans this as SortAggregateExec, because a string min/max buffer is not hash-friendly.
query
SELECT min(i), max(i), min(d), max(d), min(s), max(s) FROM test_min_max

query
SELECT grp, min(i), max(i) FROM test_min_max GROUP BY grp ORDER BY grp

-- Spark compares strings and binary as unsigned bytes, and a value sorts before any longer value
-- it is a prefix of. The groups cover prefixes, case, the empty value, characters whose UTF-8
-- order differs from their UTF-16 order ('😀' sorts after 'ｱ' by bytes but before it by UTF-16
-- code units), bytes that a signed comparison would order the other way (X'80' after X'7F'),
-- and a group of only NULLs.
statement
CREATE TABLE test_min_max_bytes(grp string, s string, b binary) USING parquet

statement
INSERT INTO test_min_max_bytes VALUES
  ('prefix', 'ab', X'0102'), ('prefix', 'a', X'01'), ('prefix', NULL, NULL),
  ('case', 'a', X'61'), ('case', 'B', X'42'), ('case', 'A', X'41'),
  ('empty', ' ', X'20'), ('empty', '', X''), ('empty', NULL, NULL),
  ('utf8', 'ｱ', X'7F'), ('utf8', '😀', X'80'), ('utf8', 'é', X'FF'), ('utf8', 'z', X'00'),
  ('nulls', NULL, NULL), ('nulls', NULL, NULL)

query
SELECT grp, min(s), max(s), min(b), max(b) FROM test_min_max_bytes GROUP BY grp ORDER BY grp

query
SELECT min(s), max(s), min(b), max(b) FROM test_min_max_bytes

query
SELECT min(s), max(s), min(b), max(b) FROM test_min_max_bytes WHERE grp = 'missing'

-- collect_set makes Spark plan ObjectHashAggregateExec instead, with min/max in the same operator.
query
SELECT grp, min(s), max(b), sort_array(collect_set(s))
FROM test_min_max_bytes GROUP BY grp ORDER BY grp

-- count(DISTINCT) makes Spark plan four sort aggregates, so the string and binary buffers also go
-- through a PartialMerge.
query
SELECT grp, count(DISTINCT s), min(s), max(b)
FROM test_min_max_bytes GROUP BY grp ORDER BY grp

-- A VALUES input keeps the partial aggregate on Spark, while the final one merges Spark's string
-- and binary buffers natively. The plan is not fully native, so only the answer is checked.
query spark_answer_only
SELECT k, min(s), max(s), min(b), max(b)
FROM VALUES (1, 'ab', X'0102'), (1, 'a', X'01'), (2, NULL, NULL), (2, '😀', X'80'), (2, 'ｱ', X'7F')
  AS t(k, s, b)
GROUP BY k ORDER BY k
