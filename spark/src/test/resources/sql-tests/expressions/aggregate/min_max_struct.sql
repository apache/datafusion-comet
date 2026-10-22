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
