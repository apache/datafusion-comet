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

-- WindowGroupLimit data-type coverage for ORDER BY / PARTITION BY keys.
-- Exercises int, bigint (past Int32), double (NaN, +/-Inf, +/-0.0), decimal, date,
-- timestamp, string (incl. empty and NULL). Also covers decimal and date partition keys,
-- which upstream Spark's window.sql does not exercise.
-- ROW_NUMBER cases execute natively via `CometWindowGroupLimitExec`; the RANK and DENSE_RANK
-- cases are exercised here for coverage and remain fall-back until we grow a rank/dense-rank
-- aware native operator.
--
-- Each ROW_NUMBER's ORDER BY carries `payload ASC` as a secondary key because `payload` is
-- unique per row; that removes tie-breaking non-determinism between Spark's stream-based
-- SimpleLimitIterator and DataFusion's heap-based PartitionedTopKExec (see comment in
-- window_group_limit_row_number.sql).

-- MinSparkVersion: 3.5
-- ConfigMatrix: spark.sql.optimizer.windowGroupLimitThreshold=-1,1000

statement
CREATE TABLE test_wgl_types(
  k_str  string,
  k_int  int,
  k_long bigint,
  k_dbl  double,
  k_dec  decimal(18,4),
  k_date date,
  k_ts   timestamp,
  payload int
) USING parquet

statement
INSERT INTO test_wgl_types VALUES
  ('a',  1,           2147483650,           1.5,               cast('1.2345'               as decimal(18,4)), date '2020-01-01', timestamp '2020-01-01 00:00:00', 1),
  ('a',  2,           2147483651,           double('NaN'),     cast('99999999999999.9999'  as decimal(18,4)), date '2020-01-02', timestamp '2020-01-01 00:00:01', 2),
  ('a', -2147483648, -9223372036854775808,  double('Infinity'),cast('-99999999999999.9999' as decimal(18,4)), date '1970-01-01', timestamp '1970-01-01 00:00:00', 3),
  ('a',  2147483647,  9223372036854775807,  double('-Infinity'),cast('0.0000'              as decimal(18,4)), date '9999-12-31', timestamp '9999-12-31 23:59:59', 4),
  ('a',  0,           0,                    0.0,               cast('0.0001'               as decimal(18,4)), date '2020-06-15', timestamp '2020-06-15 12:00:00', 5),
  ('a',  0,           0,                   -0.0,               cast('-0.0001'              as decimal(18,4)), date '2020-06-15', timestamp '2020-06-15 12:00:00', 6),
  ('b',  10,          10,                   double('NaN'),     cast('1.0'                  as decimal(18,4)), date '2020-01-01', timestamp '2020-01-01 00:00:00', 7),
  ('b',  NULL,        NULL,                 NULL,              NULL,                                          NULL,              NULL,                            8),
  ('',   42,          42,                   42.0,              cast('42'                   as decimal(18,4)), date '2000-02-29', timestamp '2000-02-29 03:14:15', 9),
  (NULL, 1,           1,                    1.0,               cast('1'                    as decimal(18,4)), date '2000-01-01', timestamp '2000-01-01 00:00:00', 10)

-- Order by int, top-1 per string partition.
query
SELECT k_str, k_int, payload FROM (
  SELECT k_str, k_int, payload,
         ROW_NUMBER() OVER (PARTITION BY k_str ORDER BY k_int DESC NULLS LAST, payload ASC) AS rn
  FROM test_wgl_types
) t WHERE rn = 1 ORDER BY k_str NULLS LAST

-- Order by bigint past Int32 range.
query
SELECT k_str, k_long, payload FROM (
  SELECT k_str, k_long, payload,
         ROW_NUMBER() OVER (PARTITION BY k_str ORDER BY k_long ASC NULLS LAST, payload ASC) AS rn
  FROM test_wgl_types
) t WHERE rn <= 2 ORDER BY k_str NULLS LAST, rn

-- Order by double (NaN, +/-Inf, +/-0.0). Spark treats NaN as max, +0.0 == -0.0.
query
SELECT k_str, k_dbl, payload FROM (
  SELECT k_str, k_dbl, payload,
         ROW_NUMBER() OVER (PARTITION BY k_str ORDER BY k_dbl DESC NULLS LAST, payload ASC) AS rn
  FROM test_wgl_types
) t WHERE rn <= 3 ORDER BY k_str NULLS LAST, rn

-- Order by decimal.
query
SELECT k_str, k_dec, payload FROM (
  SELECT k_str, k_dec, payload,
         ROW_NUMBER() OVER (PARTITION BY k_str ORDER BY k_dec DESC NULLS LAST, payload ASC) AS rn
  FROM test_wgl_types
) t WHERE rn <= 2 ORDER BY k_str NULLS LAST, rn

-- Order by date.
query
SELECT k_str, k_date, payload FROM (
  SELECT k_str, k_date, payload,
         ROW_NUMBER() OVER (PARTITION BY k_str ORDER BY k_date ASC NULLS FIRST, payload ASC) AS rn
  FROM test_wgl_types
) t WHERE rn <= 2 ORDER BY k_str NULLS LAST, rn

-- Order by timestamp.
query
SELECT k_str, k_ts, payload FROM (
  SELECT k_str, k_ts, payload,
         ROW_NUMBER() OVER (PARTITION BY k_str ORDER BY k_ts DESC NULLS LAST, payload ASC) AS rn
  FROM test_wgl_types
) t WHERE rn <= 2 ORDER BY k_str NULLS LAST, rn

-- Order by string (incl. empty string and NULL); partition by a derived int.
query
SELECT k_str, payload FROM (
  SELECT k_str, payload,
         ROW_NUMBER() OVER (PARTITION BY payload % 3 ORDER BY k_str ASC NULLS FIRST, payload ASC) AS rn
  FROM test_wgl_types
) t WHERE rn <= 2 ORDER BY payload % 3, rn

-- Decimal partition key. RANK is not yet supported natively; falls back to Spark under
-- threshold=1000, runs via CometWindow+Filter under threshold=-1. Parity-only assertion.
query spark_answer_only
SELECT k_dec, k_int FROM (
  SELECT k_dec, k_int,
         RANK() OVER (PARTITION BY k_dec ORDER BY k_int DESC NULLS LAST) AS rk
  FROM test_wgl_types
) t WHERE rk = 1 ORDER BY k_dec NULLS LAST, k_int DESC NULLS LAST

-- Date partition key with DENSE_RANK. Parity-only (see comment above).
query spark_answer_only
SELECT k_date, k_int FROM (
  SELECT k_date, k_int,
         DENSE_RANK() OVER (PARTITION BY k_date ORDER BY k_int ASC NULLS LAST) AS dr
  FROM test_wgl_types
) t WHERE dr <= 2 ORDER BY k_date NULLS LAST, dr, k_int NULLS LAST

-- Partition column IS the order column.
query
SELECT k_str, k_int FROM (
  SELECT k_str, k_int,
         ROW_NUMBER() OVER (PARTITION BY k_str ORDER BY k_str, k_int ASC NULLS LAST, payload ASC) AS rn
  FROM test_wgl_types
) t WHERE rn = 1 ORDER BY k_str NULLS LAST
