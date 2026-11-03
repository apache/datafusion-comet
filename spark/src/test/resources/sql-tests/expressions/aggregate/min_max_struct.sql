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

-- min/max over a struct. Spark orders structs field by field, with a null field (or a null
-- nested struct) below every value, and skips a null struct entirely. Float and double fields
-- fall back.

statement
CREATE TABLE test_mm_struct(g int, ts bigint, s string, d decimal(38,0), dt date, t timestamp, n string)
USING parquet

statement
INSERT INTO test_mm_struct VALUES
  (1, 5, 'b', 99999999999999999999999999999999999999, DATE'1980-01-01', TIMESTAMP'1980-01-01 00:00:00', 'x'),
  (1, 9, '日本語', -99999999999999999999999999999999999999, DATE'9999-12-31', TIMESTAMP'2100-01-01 00:00:00', 'y'),
  (1, 9, '中文', 0, DATE'2000-02-29', TIMESTAMP'2000-02-29 12:00:00', NULL),
  (2, 9223372036854775807, 'a', 1, DATE'1980-01-01', TIMESTAMP'1999-12-31 23:59:59', 'z'),
  (2, -9223372036854775808, 'c', 2, DATE'1980-01-02', TIMESTAMP'1999-12-31 23:59:58', 'w'),
  (3, 1, 'only', 3, DATE'2020-01-01', TIMESTAMP'2020-01-01 00:00:00', NULL)

-- Non-nullable fields.
query
SELECT g, min(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)),
  max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s))
FROM test_mm_struct GROUP BY g ORDER BY g

query
SELECT g, min(struct(coalesce(d, 0BD) AS d, coalesce(dt, DATE'1970-01-01') AS dt, coalesce(t, TIMESTAMP'1970-01-01 00:00:00') AS t)),
  max(struct(coalesce(d, 0BD) AS d, coalesce(dt, DATE'1970-01-01') AS dt, coalesce(t, TIMESTAMP'1970-01-01 00:00:00') AS t))
FROM test_mm_struct GROUP BY g ORDER BY g

-- Nested struct, and a whole-struct null from a guarding CASE WHEN.
query
SELECT g, max(CASE WHEN ts > 0 THEN struct(coalesce(ts, 0L) AS ts, struct(coalesce(s, '') AS s, true AS b) AS inner) END)
FROM test_mm_struct GROUP BY g ORDER BY g

-- No grouping.
query
SELECT min(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)) FROM test_mm_struct

-- Float and double fields fall back: Comet orders -0.0 below 0.0, and a NaN with the sign bit
-- set below every value.
query expect_fallback(float or double field)
SELECT g, max(struct(coalesce(ts, 0L) AS ts, 1.5D AS d)) FROM test_mm_struct GROUP BY g ORDER BY g

-- Array field: unsupported.
query expect_fallback(Struct min/max over a field of type ArrayType)
SELECT g, max(struct(coalesce(ts, 0L) AS ts, array(1, 2) AS a)) FROM test_mm_struct GROUP BY g ORDER BY g

-- Spark cannot order maps, so it rejects a struct holding one during analysis.
query expect_error(INVALID_ORDERING_TYPE)
SELECT g, max(struct(coalesce(ts, 0L) AS ts, map('k', 1) AS m)) FROM test_mm_struct GROUP BY g ORDER BY g

-- timestamp_ntz is not supported by native min/max, so a struct holding one falls back too.
query expect_fallback(Struct min/max over a field of type TimestampNTZType)
SELECT g, max(struct(coalesce(ts, 0L) AS ts, TIMESTAMP_NTZ'2020-01-01 00:00:00' AS t)) FROM test_mm_struct GROUP BY g ORDER BY g

-- Boolean, integral and binary fields.
query
SELECT g,
  min(struct(coalesce(ts, 0L) > 5 AS b, coalesce(CAST(ts % 100 AS TINYINT), 0Y) AS i8,
    coalesce(CAST(ts % 30000 AS SMALLINT), 0S) AS i16, coalesce(CAST(ts % 100000 AS INT), 0) AS i32,
    coalesce(CAST(s AS BINARY), X'') AS bin)),
  max(struct(coalesce(ts, 0L) > 5 AS b, coalesce(CAST(ts % 100 AS TINYINT), 0Y) AS i8,
    coalesce(CAST(ts % 30000 AS SMALLINT), 0S) AS i16, coalesce(CAST(ts % 100000 AS INT), 0) AS i32,
    coalesce(CAST(s AS BINARY), X'') AS bin))
FROM test_mm_struct GROUP BY g ORDER BY g

-- Ties on the first field are decided by the next one. Group 1 has two rows with ts = 9, and
-- '中文' sorts before '日本語' as raw UTF-8 bytes.
query
SELECT g, min(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)),
  max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s))
FROM test_mm_struct WHERE ts = 9 GROUP BY g ORDER BY g

-- count(DISTINCT) makes Spark plan a PartialMerge stage, which merges the struct buffers again.
query
SELECT g, count(DISTINCT s), max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s))
FROM test_mm_struct GROUP BY g ORDER BY g

-- A VALUES input keeps the partial aggregate on Spark, while the final one merges Spark's struct
-- buffers natively. The plan is not fully native, so only the answer is checked.
query spark_answer_only
SELECT k, min(struct(o, v)), max(struct(o, v))
FROM VALUES (1, 5L, 'b'), (1, 9L, 'ｱ'), (2, -1L, '😀'), (2, 3L, 'a') AS t(k, o, v)
GROUP BY k ORDER BY k

-- Empty input: one NULL row without grouping, no rows with grouping.
query
SELECT max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)) FROM test_mm_struct WHERE g < 0

query
SELECT g, max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)) FROM test_mm_struct WHERE g < 0 GROUP BY g

-- FILTER turns filtered-out rows into nulls before they reach the struct accumulator.
query
SELECT g, max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)) FILTER (WHERE ts < 9)
FROM test_mm_struct GROUP BY g ORDER BY g

-- A nested struct that can be null orders below every non-null nested struct.
query
SELECT g, max(struct(coalesce(ts, 0L) AS ts, CASE WHEN ts > 5 THEN struct(coalesce(s, '') AS s) END AS inner))
FROM test_mm_struct GROUP BY g ORDER BY g

-- Null fields at every level.
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
