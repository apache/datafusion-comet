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

-- min/max over a struct. Compatible only when no field can be null and no field is float/double.
-- Structs with nullable fields are opt-in because DataFusion skips null fields when comparing.

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

-- Non-nullable fields via coalesce: native.
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

-- Nullable fields: opt-in, falls back by default.
query expect_fallback(Struct min/max with nullable fields)
SELECT g, max(struct(ts, s, n)) FROM test_mm_struct GROUP BY g ORDER BY g

-- Float and double fields: NaN ordering differs, so fall back.
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

-- Boolean, integral and binary fields. Spark marks narrowing casts nullable, so each one is
-- wrapped in coalesce to keep the struct Compatible.
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

-- A nested struct that can be null makes the outer struct opt-in.
query expect_fallback(Struct min/max with nullable fields)
SELECT g, max(struct(coalesce(ts, 0L) AS ts, CASE WHEN ts > 5 THEN struct(coalesce(s, '') AS s) END AS inner))
FROM test_mm_struct GROUP BY g ORDER BY g

-- Spark reads file-source schemas as nullable, so min/max over a struct column read from a table
-- is opt-in even when every stored value is non-null.
statement
CREATE TABLE test_mm_struct_col(g int, st struct<ts: bigint, s: string>) USING parquet

statement
INSERT INTO test_mm_struct_col VALUES
  (1, named_struct('ts', 5L, 's', 'b')), (1, named_struct('ts', 9L, 's', '日本語')),
  (2, named_struct('ts', 1L, 's', 'a')), (2, NULL)

query expect_fallback(Struct min/max with nullable fields)
SELECT g, min(st), max(st) FROM test_mm_struct_col GROUP BY g ORDER BY g
