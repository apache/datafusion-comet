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

-- Spark's TopK ordering evaluates a later sort key only for rows whose earlier keys tie, so a
-- later key that can fail does not fail on a row Spark skips. Comet evaluates every key over the
-- whole batch, so a TopK whose later key can fail runs on Spark
-- (https://github.com/apache/datafusion-comet/issues/7246).
-- Each table is one batch.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS

statement
CREATE TABLE topk(a int, b int) USING parquet

statement
INSERT INTO topk SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, 2147483647), (2, 5), (3, 2147483647), (4, 7), (5, 9) AS t(a, b)

-- No two rows tie on a, so Spark never evaluates b + 1
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk ORDER BY a, b + 1 LIMIT 2

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk ORDER BY a DESC, CAST(CAST(b AS STRING) AS INT) * 2 LIMIT 2

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk ORDER BY a, abs(b) LIMIT 2

-- Later keys that cannot fail stay in Comet
query
SELECT a, b FROM topk ORDER BY a, b LIMIT 2

query
SELECT a, b FROM topk ORDER BY a, b IS NULL, CAST(b AS BIGINT), try_add(b, 1) LIMIT 2

query
SELECT a, b FROM topk ORDER BY a, substr(CAST(b AS STRING), 1, 3), upper(CAST(b AS STRING)),
  length(lower(CAST(b AS STRING))) LIMIT 2

-- A later key that can fail falls back even when an allowed expression wraps it
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk ORDER BY a, coalesce(b + 1, 0) LIMIT 2

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk ORDER BY a, IF(a > 3, b + 1, 0) LIMIT 2

-- Rounding an integer overflows under ANSI mode
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk ORDER BY a, round(b, -1) LIMIT 2

query
SELECT a, b FROM topk ORDER BY a, try_cast(CAST(b AS STRING) AS INT) LIMIT 2

statement
CREATE TABLE topk_types(a int, b int, l bigint, d double, s string, dt date, ts timestamp)
USING parquet

statement
INSERT INTO topk_types SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, 2147483647, 9223372036854775807, double('NaN'), ' x ', date'2024-01-31',
    timestamp'2024-01-31 23:59:59'),
  (2, NULL, NULL, -0.0D, NULL, NULL, NULL),
  (3, -2147483648, -9223372036854775808, 0.0D, 'abc', date'1970-01-01',
    timestamp'1970-01-01 00:00:00'),
  (4, 7, 7, 1.5D, 'Spark', date'2000-02-29', timestamp'2000-02-29 12:30:45'),
  (5, 0, 0, -1.0E308D, '', date'9999-12-31', timestamp'9999-12-31 23:59:59')
  AS t(a, b, l, d, s, dt, ts)

-- Later keys built from functions that cannot fail stay in Comet
query
SELECT a, s FROM topk_types ORDER BY a, trim(s), ltrim(s), rtrim(s), replace(s, 'a', 'b'),
  reverse(s), octet_length(s), bit_length(s), concat(s, 'x'), concat_ws('-', s, s) LIMIT 3

query
SELECT a, dt FROM topk_types ORDER BY a, year(dt), quarter(dt), month(dt), dayofmonth(dt),
  dayofweek(dt), weekday(dt), dayofyear(dt), hour(ts), minute(ts), second(ts) LIMIT 3

query
SELECT a, d FROM topk_types ORDER BY a, floor(d), ceil(d), sqrt(d), exp(d), ln(d), signum(d),
  isnan(d), nanvl(d, 0D), round(d, 1), greatest(b, 0), least(l, 0), b IN (1, 2) LIMIT 3

query
SELECT a, s FROM topk_types ORDER BY a, hash(b, s), xxhash64(s), md5(s), sha1(s), sha2(s, 256),
  crc32(s) LIMIT 3

query
SELECT a, s FROM topk_types ORDER BY a, initcap(s) LIMIT 3

-- Comet's Rust timestamp casts reject years that Spark accepts, even for TRY
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, ts FROM topk_types ORDER BY a, CAST(ts AS STRING) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, ts FROM topk_types ORDER BY a, try_cast(ts AS DATE) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, dt FROM topk_types ORDER BY a, try_cast(dt AS TIMESTAMP) LIMIT 3

-- Comet's Rust date formatting rejects years that Spark accepts, in every mode
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, dt FROM topk_types ORDER BY a, CAST(dt AS STRING) LIMIT 3

statement
CREATE TABLE topk_nested(a int, tsa array<timestamp>, dta array<date>, sts struct<f: timestamp>,
  std struct<f: date>, md map<int, date>, mt map<timestamp, int>, ast array<struct<f: timestamp>>)
USING parquet

statement
INSERT INTO topk_nested SELECT /*+ REPARTITION(1) */ a, array(ts), array(dt), named_struct('f', ts),
  named_struct('f', dt), map(1, dt), map(ts, 1), array(named_struct('f', ts)) FROM VALUES
  (1, timestamp_micros(9000000000000000000), date_from_unix_date(100000000)),
  (1, timestamp'2024-01-31 23:59:59', date'2024-01-31'),
  (2, timestamp'1970-01-01 00:00:00', date'1970-01-01') AS t(a, ts, dt)

-- The same Rust kernels cast each element, field, key and value of a nested type
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(tsa AS STRING) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(dta AS STRING) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(tsa AS array<string>) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(dta AS array<string>) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(sts AS STRING) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(std AS STRING) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(std AS struct<f: string>) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(sts AS struct<f: date>) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(std AS struct<f: timestamp>) LIMIT 3

-- A map cannot be a sort key, so test it under IS NULL, which cannot fail by itself
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(md AS map<int, string>) IS NULL LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(mt AS map<string, int>) IS NULL LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_nested ORDER BY a, try_cast(ast AS STRING) LIMIT 3

-- Floating-point arithmetic cannot fail even under ANSI mode
query
SELECT a, d FROM topk_types ORDER BY a, d * 1.1D, d + 1, d - 1, -d, abs(d) LIMIT 3

statement
CREATE TABLE topk_struct(a int, s struct<x: int, y: string>) USING parquet

statement
INSERT INTO topk_struct SELECT /*+ REPARTITION(1) */ a, IF(x IS NULL, NULL, named_struct('x', x,
  'y', y)) FROM VALUES (1, 2147483647, 'p'), (1, 5, 'q'), (2, NULL, NULL), (3, -1, '') AS t(a, x, y)

-- Reading a struct field cannot fail
query
SELECT a, s FROM topk_struct ORDER BY a, s.x, s.y LIMIT 3

-- The first key is evaluated for every row in Spark too when the input has at least two rows
-- (https://github.com/apache/datafusion-comet/issues/7527)
query
SELECT a, b FROM topk ORDER BY a + 1, b LIMIT 2

statement
CREATE TABLE topk_ties(a int, b int) USING parquet

statement
INSERT INTO topk_ties SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, 2147483647), (1, 5), (2, 7) AS t(a, b)

-- Rows tie on a, so Spark evaluates b + 1 and fails
query expect_error(ARITHMETIC_OVERFLOW)
SELECT a, b FROM topk_ties ORDER BY a, b + 1 LIMIT 2

-- Without ANSI mode the key cannot fail and stays in Comet
statement
SET spark.sql.ansi.enabled=false

query
SELECT a, b FROM topk ORDER BY a, b + 1 LIMIT 2

query
SELECT a, b FROM topk_ties ORDER BY a, b + 1 LIMIT 2

query
SELECT a, b FROM topk_ties ORDER BY a, -b, abs(b) LIMIT 2

query
SELECT a, b FROM topk ORDER BY a, round(b, -1), CAST(CAST(b AS STRING) AS INT) LIMIT 2

query
SELECT a, b FROM topk_types ORDER BY a, b / 0, b % 0, b DIV 0, l + 1, l * 2 LIMIT 3

query
SELECT a, s FROM topk_struct ORDER BY a, s.x, s.y LIMIT 3

-- Without ANSI mode a later key fails over to Spark only when it can raise in any mode
query
SELECT a, dt FROM topk_types ORDER BY a, date_add(dt, 1) LIMIT 3

query
SELECT a, s FROM topk_types ORDER BY a, lpad(s, 5) LIMIT 3

-- Spark adds casts to and from a timestamp quietly, and with ANSI off they give NULL rather
-- than raise, so these keys stay in Comet
query
SELECT a, ts FROM topk_types ORDER BY a, date_add(ts, 1) LIMIT 3

query
SELECT a, ts FROM topk_types ORDER BY a, concat(s, ts) LIMIT 3

statement
CREATE TABLE topk_arrays(a int, arr array<int>) USING parquet

statement
INSERT INTO topk_arrays SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, array(1, 2)), (2, array(3)), (3, NULL), (4, array()) AS t(a, arr)

-- element_at(array, 0) raises INVALID_INDEX_OF_ZERO in every mode
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_arrays ORDER BY a, element_at(arr, 0) LIMIT 3

-- A constant index other than 0 gives NULL when out of bounds
query
SELECT a FROM topk_arrays ORDER BY a, element_at(arr, 1) LIMIT 3

-- slice raises for a start of 0, array_insert for a position of 0, and sequence for bounds that
-- do not match the step, in every mode. No two rows tie on a, so Spark never evaluates them.
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_arrays ORDER BY a, size(slice(arr, a - 1, 1)) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_arrays ORDER BY a, size(array_insert(arr, a - 1, 0)) LIMIT 3

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_arrays ORDER BY a, size(sequence(a, 1, 1)) LIMIT 3

query
SELECT a FROM topk_arrays ORDER BY a, size(slice(arr, 1, 1)), size(array_insert(arr, 1, 0))
LIMIT 3

statement
CREATE TABLE topk_regex(a int, s string, p string) USING parquet

statement
INSERT INTO topk_regex SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, 'abc', 'a.*'), (2, 'abc', '('), (3, 'xyz', 'x'), (4, NULL, NULL) AS t(a, s, p)

-- rlike raises for an invalid pattern in every mode. No two rows tie on a, so Spark never
-- evaluates the pattern '(' on row 2.
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a FROM topk_regex ORDER BY a, s RLIKE p LIMIT 3

query
SELECT a FROM topk_regex ORDER BY a, s RLIKE '^a' LIMIT 3

-- A view keeps the configs it was created with, so the b + 1 in its ordering stays in ANSI mode
statement
SET spark.sql.ansi.enabled=true

statement
CREATE TEMPORARY VIEW topk_ansi_view AS SELECT a, b FROM topk ORDER BY a, b + 1 LIMIT 2

statement
SET spark.sql.ansi.enabled=false

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk_ansi_view

statement
DROP VIEW topk_ansi_view
