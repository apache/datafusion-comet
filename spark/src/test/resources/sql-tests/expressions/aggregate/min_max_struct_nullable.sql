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

-- min/max over a struct with nullable fields, opted in with allowIncompatible.
-- DataFusion skips a field when either side is null and moves on to the next field, while Spark
-- orders null below every value. The answers match as long as the field that decides the order is
-- not null, which is what these queries check.

-- Config: spark.comet.expression.Min.allowIncompatible=true
-- Config: spark.comet.expression.Max.allowIncompatible=true

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
  (4, NULL, NULL, NULL, NULL)

-- The deciding field ts is distinct and non-null within each group; later fields may be null.
query
SELECT g, min(struct(ts, s, n)), max(struct(ts, s, n))
FROM test_mm_struct_null WHERE ts IS NOT NULL GROUP BY g ORDER BY g

-- Nested struct with nullable inner fields.
query
SELECT g, min(struct(ts, struct(s, inner_v) AS inner)), max(struct(ts, struct(s, inner_v) AS inner))
FROM test_mm_struct_null WHERE ts IS NOT NULL GROUP BY g ORDER BY g

-- Whole-struct nulls are skipped by both engines. Group 4 has only a NULL struct, so the result
-- is NULL.
query
SELECT g, max(CASE WHEN ts IS NOT NULL THEN struct(ts, s) END)
FROM test_mm_struct_null GROUP BY g ORDER BY g

-- Known difference: in group 1 Spark returns (7, 'a') because a null ts sorts first. DataFusion
-- skips the null ts and compares the second field, where '~' is larger than 'a', so it returns
-- (NULL, '~').
query ignore(DataFusion skips null struct fields when comparing, Spark orders null first)
SELECT max(struct(CASE WHEN ts = 9 THEN NULL ELSE ts END AS ts,
  CASE WHEN ts = 9 THEN '~' ELSE 'a' END AS s))
FROM test_mm_struct_null WHERE g = 1

-- A struct column read from a table. Spark reads it with nullable fields, so it goes through the
-- opt-in path and the cast that relabels its fields.
statement
CREATE TABLE test_mm_struct_null_col(g int, st struct<ts: bigint, s: string>) USING parquet

statement
INSERT INTO test_mm_struct_null_col VALUES
  (1, named_struct('ts', 5L, 's', 'b')), (1, named_struct('ts', 9L, 's', NULL)),
  (1, named_struct('ts', 7L, 's', '日本語')), (2, named_struct('ts', 1L, 's', 'a')), (2, NULL)

query
SELECT g, min(st), max(st) FROM test_mm_struct_null_col GROUP BY g ORDER BY g
