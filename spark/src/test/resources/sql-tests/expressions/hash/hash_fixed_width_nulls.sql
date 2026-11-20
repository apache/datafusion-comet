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

-- Plain encoding keeps the columns off the dictionary path, so they reach the 64-row chunk loop
-- in `hash_fixed_width_with_nulls`.
-- Config: parquet.enable.dictionary=false

-- One file of 1300 rows, so each block of 256 rows is four aligned 64-row chunks and the last 20
-- rows are a partial chunk. Each block has its own validity pattern. Blocks 1 and 2 have 16 and
-- 15 valid rows per chunk, one at and one below `MIN_VALID_FOR_DENSE`, so they choose different
-- branches. Update them if that constant changes. Each column is a type that reaches the loop
-- through its own instantiation of `hash_array_primitive` or `hash_array_primitive_float`.
statement
CREATE TABLE test_hash_fixed_width_nulls USING parquet AS
SELECT /*+ COALESCE(1) */ id, IF(is_null, NULL, CAST(id * 7919 - 5000000 AS INT)) AS i,
  IF(is_null, NULL, id * 1000000007 - 1000000000000) AS l,
  IF(is_null, NULL, dv) AS d,
  IF(is_null, NULL, CAST(dv AS FLOAT)) AS f,
  IF(is_null, NULL, CAST(CASE id % 89 WHEN 0 THEN -128 WHEN 1 THEN 127 ELSE id % 256 - 128 END
    AS TINYINT)) AS b,
  IF(is_null, NULL, CAST(CASE id % 89 WHEN 0 THEN -32768 WHEN 1 THEN 32767
    ELSE id * 37 % 65536 - 32768 END AS SMALLINT)) AS s,
  IF(is_null, NULL, date_add(DATE '1969-12-01', CAST(id * 13 AS INT) - 5000)) AS dt,
  IF(is_null, NULL, timestamp_micros(id * 997002991 - 500000000000)) AS ts
FROM (
  SELECT id, CASE CAST(id DIV 256 AS INT)
    WHEN 0 THEN id % 256 = 128
    WHEN 1 THEN id % 64 < 48
    WHEN 2 THEN id % 64 <= 48
    WHEN 3 THEN id % 3 = 0
    WHEN 4 THEN id % 256 BETWEEN 1 AND 69
    ELSE id % 2 = 0
  END AS is_null,
  CASE id % 97 WHEN 0 THEN double('-0.0') WHEN 1 THEN double('NaN')
    WHEN 2 THEN double('Infinity') WHEN 3 THEN double('-Infinity') ELSE id / 7.0 - 50 END AS dv
  FROM range(0, 1300, 1, 1)
)

-- A float argument keeps xxhash64 on Comet's kernel at the default seed. Negating a column sets
-- the sign bit of its NaN rows.
query expect_native(hash, xxhash64)
SELECT id, hash(i), hash(l), hash(d), hash(-d), hash(f), hash(-f), hash(b), hash(s), hash(dt),
  hash(ts), hash(b, s, dt, ts, f), xxhash64(d), xxhash64(-d), xxhash64(f), xxhash64(-f),
  xxhash64(i, d), xxhash64(b, s, dt, ts, f)
FROM test_hash_fixed_width_nulls ORDER BY id
