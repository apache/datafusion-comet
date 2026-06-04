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

-- MinSparkVersion: 4.0

statement
CREATE TABLE test_split_part(s string, d string, p int) USING parquet

statement
INSERT INTO test_split_part VALUES
  ('one.two.three', '.', 2),
  ('a||b||', '||', 3),
  ('abc', '', 1),
  (NULL, '.', 1),
  ('abc', NULL, 1)

query
SELECT split_part(s, d, p) FROM test_split_part

-- literal delimiter: delimiter is literal, not regex
query
SELECT split_part('a.b.c', '.', 2), split_part('a|b|c', '|', 3)

-- negative part numbers select from the end
query
SELECT split_part('one/two/three', '/', -1), split_part('one/two/three', '/', -2)

-- out-of-range part numbers return the element_at default
query
SELECT split_part('a.b', '.', 4), split_part('a.b', '.', -4)

-- literal NULL arguments still produce a typed native array child for element_at
query
SELECT split_part(CAST(NULL AS STRING), '.', 1), split_part('abc', CAST(NULL AS STRING), 1)

-- part number zero follows element_at semantics
query expect_error(INVALID_INDEX_OF_ZERO)
SELECT split_part('a.b', '.', 0)

-- StringSplitSQL is collation-aware in Spark. Comet does not support non-default collations yet.
query expect_fallback(StringSplitSQL does not support non-UTF8_BINARY collations)
SELECT split_part('Hello' COLLATE UTF8_LCASE, 'L' COLLATE UTF8_LCASE, 2)
