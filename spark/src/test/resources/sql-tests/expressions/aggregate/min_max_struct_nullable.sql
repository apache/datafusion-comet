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

-- min/max over structs with nullable fields. Spark orders structs field by field, with a null
-- field (or a null nested struct) below every value, and skips a null struct entirely.

statement
CREATE TABLE test_mm_struct_null(g int, ts bigint, s string, n string, inner_v int) USING parquet

statement
INSERT INTO test_mm_struct_null VALUES
  (1, 5, 'b', 'x', 1),
  (1, 9, NULL, NULL, NULL),
  (1, 7, '日本語', NULL, 3),
  (2, 1, 'a', 'z', NULL),
  (2, 2, NULL, 'w', 2),
  (3, 4, NULL, NULL, NULL),
  (4, NULL, NULL, NULL, NULL),
  (5, NULL, 'z', 'p', 1),
  (5, 1, 'a', 'q', 2),
  (5, 1, NULL, 'r', 3),
  (6, 3, 'a', NULL, NULL),
  (6, 3, 'a', 'b', NULL)

-- Nulls in every position, including the deciding field: group 5 orders (NULL, ...) first and
-- breaks the tie on ts = 1 with the null s first.
query
SELECT g, min(struct(ts, s, n)), max(struct(ts, s, n))
FROM test_mm_struct_null GROUP BY g ORDER BY g

-- Nested struct with nullable inner fields, and a nested struct that is itself null.
query
SELECT g,
  min(struct(ts, CASE WHEN inner_v IS NOT NULL THEN struct(s, inner_v) END AS inner)),
  max(struct(ts, CASE WHEN inner_v IS NOT NULL THEN struct(s, inner_v) END AS inner))
FROM test_mm_struct_null GROUP BY g ORDER BY g

-- Whole-struct nulls are skipped. Group 4 has only a NULL struct, so the result is NULL.
query
SELECT g, max(CASE WHEN ts IS NOT NULL THEN struct(ts, s) END)
FROM test_mm_struct_null GROUP BY g ORDER BY g

-- A null deciding field sorts first: Spark returns (7, 'a') for max and (NULL, '~') for min.
query
SELECT min(struct(CASE WHEN ts = 9 THEN NULL ELSE ts END AS ts,
    CASE WHEN ts = 9 THEN '~' ELSE 'a' END AS s)),
  max(struct(CASE WHEN ts = 9 THEN NULL ELSE ts END AS ts,
    CASE WHEN ts = 9 THEN '~' ELSE 'a' END AS s))
FROM test_mm_struct_null WHERE g = 1

-- A struct column read from a table, whose fields Spark reads as nullable.
statement
CREATE TABLE test_mm_struct_null_col(g int, st struct<ts: bigint, s: string>) USING parquet

statement
INSERT INTO test_mm_struct_null_col VALUES
  (1, named_struct('ts', 5L, 's', 'b')), (1, named_struct('ts', 9L, 's', NULL)),
  (1, named_struct('ts', 7L, 's', '日本語')), (2, named_struct('ts', 1L, 's', 'a')), (2, NULL),
  (3, named_struct('ts', NULL, 's', 'x')), (3, named_struct('ts', 2L, 's', NULL))

query
SELECT g, min(st), max(st) FROM test_mm_struct_null_col GROUP BY g ORDER BY g
