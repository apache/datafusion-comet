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

-- RANGE frames with an explicit PRECEDING / FOLLOWING offset on a DECIMAL
-- ORDER BY (#4834). Every query runs with ANSI mode on and off.

-- ConfigMatrix: spark.sql.ansi.enabled=true,false

-- ============================================================
-- 1: ordinary frames, which run natively. Spark casts the offset to the
-- ORDER BY decimal type, and DataFusion compares Decimal128 values with
-- matching scales regardless of the precision the boundary arithmetic widens to.
-- ============================================================

-- 30 rows in three partitions. The fractional part rotates through
-- .00/.25/.50/.75 so RANGE peers vary, and the same values are kept at three
-- precisions and scales.
statement
CREATE TABLE dec_range(a int, d decimal(10, 2), d0 decimal(10, 0), d6 decimal(18, 6), c int) USING parquet

statement
INSERT INTO dec_range
SELECT CAST(id % 3 AS int),
       CAST(id % 10 + (id % 4) * 0.25 AS decimal(10, 2)),
       CAST(id % 10 + (id % 4) * 0.25 AS decimal(10, 0)),
       CAST(id % 10 + (id % 4) * 0.25 AS decimal(18, 6)),
       CAST(id * 2 AS int)
FROM range(30)

-- Each frame shape with an explicit offset
query
SELECT a, d, c,
  SUM(c) OVER (PARTITION BY a ORDER BY d RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS s_pf,
  SUM(c) OVER (PARTITION BY a ORDER BY d RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) AS s_pc,
  SUM(c) OVER (PARTITION BY a ORDER BY d RANGE BETWEEN CURRENT ROW AND 3 FOLLOWING) AS s_cf,
  SUM(c) OVER (PARTITION BY a ORDER BY d RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) AS s_uf
FROM dec_range

-- Descending order swaps the direction of `current +/- offset`, and a
-- fractional offset exercises scale-aware boundary arithmetic
query
SELECT a, d, c,
  SUM(c) OVER (PARTITION BY a ORDER BY d DESC RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS s_desc,
  SUM(c) OVER (PARTITION BY a ORDER BY d RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING) AS s_frac
FROM dec_range

-- Zero scale and a higher precision and scale
query
SELECT a, d0, d6, c,
  SUM(c) OVER (PARTITION BY a ORDER BY d0 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s_d0,
  SUM(c) OVER (PARTITION BY a ORDER BY d6 RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS s_d6
FROM dec_range

-- Partition 1 holds duplicate values, which RANGE (unlike ROWS) takes in as
-- peers. Partition 2 holds NULL keys, which sort first under ASC. Partition 3
-- holds negative values, so `current +/- offset` crosses zero.
statement
CREATE TABLE dec_range_edge(a int, d decimal(10, 2), c int) USING parquet

statement
INSERT INTO dec_range_edge VALUES
  (1, 1.00, 10), (1, 1.00, 20), (1, 2.00, 30), (1, 2.00, 40), (1, 5.00, 50), (1, 5.00, 60),
  (2, NULL, 10), (2, 1.00, 20), (2, 2.00, 30), (2, NULL, 40), (2, 5.00, 50),
  (3, -5.50, 10), (3, -3.00, 20), (3, -1.25, 30), (3, 0.00, 40), (3, 1.25, 50), (3, 3.00, 60),
  (3, 5.50, 70)

query
SELECT a, d, c,
  SUM(c) OVER (PARTITION BY a ORDER BY d RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s_1,
  SUM(c) OVER (PARTITION BY a ORDER BY d RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS s_2
FROM dec_range_edge

-- Maximum precision, with values and offset far enough from the Decimal128
-- limit that `current +/- offset` cannot overflow. The overflow cases are in
-- section 2.
statement
CREATE TABLE dec_range_max(a int, d decimal(38, 0), c int) USING parquet

statement
INSERT INTO dec_range_max VALUES (1, 10, 10), (1, 13, 20), (1, 15, 30), (1, 18, 40), (1, 22, 50)

query
SELECT a, d, c,
  SUM(c) OVER (PARTITION BY a ORDER BY d RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS s
FROM dec_range_max

-- ============================================================
-- 2: the Decimal128 limit. DataFusion rescales the value and the offset to
-- the larger scale and adds them as an i128, and treats a bound that
-- overflows as the edge of the partition, which also takes in the NULL keys
-- sorted there. Spark's DecimalAddNoOverflowCheck keeps the exact bound, so
-- Comet falls back when the largest value of the ORDER BY type plus the
-- offset can exceed 2^127 - 1 unscaled.
-- https://github.com/apache/datafusion-comet/issues/5022
-- ============================================================

-- 2.1: DECIMAL(38, 0) with N = 9e37. The bound 2N of the row N overflows
-- natively and would take the trailing NULL in, while Spark's bound stays
-- below it.
statement
CREATE TABLE dec_overflow_38_0(d decimal(38, 0)) USING parquet

statement
INSERT INTO dec_overflow_38_0
SELECT CAST(s AS decimal(38, 0)) FROM VALUES
  ('0'), ('90000000000000000000000000000000000000'), (NULL) AS v(s)

query expect_fallback(ORDER BY with an offset whose bound can overflow Decimal128 is not supported)
SELECT d,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST
                 RANGE BETWEEN 90000000000000000000000000000000000000 PRECEDING
                           AND 90000000000000000000000000000000000000 FOLLOWING) AS c
FROM dec_overflow_38_0

-- 2.2: DECIMAL(38, 0) at the limit, 2^127 - 1 minus the largest value of the
-- type, 70141183460469231731687303715884105728. Spark negates a PRECEDING
-- offset with 34 significant digits on Scala 2.13, which would round the
-- limit itself past it, so these frames use the largest 34-digit offset
-- under it. The type's extremes sit next to the NULL at either edge.
statement
CREATE TABLE dec_edge_38_0(d decimal(38, 0)) USING parquet

statement
INSERT INTO dec_edge_38_0
SELECT CAST(s AS decimal(38, 0)) FROM VALUES
  ('-99999999999999999999999999999999999999'), ('0'),
  ('99999999999999999999999999999999999999'), (NULL) AS v(s)

query
SELECT d,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN 70141183460469231731687303715884100000 PRECEDING AND 70141183460469231731687303715884100000 FOLLOWING) AS c0
FROM dec_edge_38_0

query
SELECT d,
  COUNT(*) OVER (ORDER BY d ASC NULLS FIRST RANGE BETWEEN 70141183460469231731687303715884100000 PRECEDING AND 70141183460469231731687303715884100000 FOLLOWING) AS c0
FROM dec_edge_38_0

query
SELECT d,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN 70141183460469231731687303715884100000 PRECEDING AND 70141183460469231731687303715884100000 FOLLOWING) AS c0
FROM dec_edge_38_0

query
SELECT d,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN 70141183460469231731687303715884100000 PRECEDING AND 70141183460469231731687303715884100000 FOLLOWING) AS c0
FROM dec_edge_38_0

-- 2.3: DECIMAL(38, 10), whose limit is the same unscaled value,
-- 7014118346046923173168730371.5884105728. One unscaled unit over it falls
-- back. A FOLLOWING offset under an ascending ORDER BY is not negated, so the
-- limit itself reaches Spark's frame exactly and runs natively. Spark negates
-- any offset under a descending ORDER BY, which rounds the limit up past it,
-- so descending frames use the largest 34-digit offset under it.
statement
CREATE TABLE dec_edge_38_10(d decimal(38, 10)) USING parquet

statement
INSERT INTO dec_edge_38_10
SELECT CAST(s AS decimal(38, 10)) FROM VALUES
  ('-9999999999999999999999999999.9999999999'), ('0'),
  ('9999999999999999999999999999.9999999999'), (NULL) AS v(s)

query expect_fallback(ORDER BY with an offset whose bound can overflow Decimal128 is not supported)
SELECT d,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND 7014118346046923173168730371.5884105729 FOLLOWING) AS c0
FROM dec_edge_38_10

query expect_fallback(ORDER BY with an offset whose bound can overflow Decimal128 is not supported)
SELECT d,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 7014118346046923173168730371.5884105729 FOLLOWING) AS c0
FROM dec_edge_38_10

query
SELECT d,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND 7014118346046923173168730371.5884105728 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN 7014118346046923173168730371.5884100000 PRECEDING AND 7014118346046923173168730371.5884100000 FOLLOWING) AS c1
FROM dec_edge_38_10

query
SELECT d,
  COUNT(*) OVER (ORDER BY d ASC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 7014118346046923173168730371.5884105728 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d ASC NULLS FIRST RANGE BETWEEN 7014118346046923173168730371.5884100000 PRECEDING AND 7014118346046923173168730371.5884100000 FOLLOWING) AS c1
FROM dec_edge_38_10

query
SELECT d,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND 7014118346046923173168730371.5884100000 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN 7014118346046923173168730371.5884100000 PRECEDING AND 7014118346046923173168730371.5884100000 FOLLOWING) AS c1
FROM dec_edge_38_10

query
SELECT d,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 7014118346046923173168730371.5884100000 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN 7014118346046923173168730371.5884100000 PRECEDING AND 7014118346046923173168730371.5884100000 FOLLOWING) AS c1
FROM dec_edge_38_10

-- ============================================================
-- 3: the offset Spark evaluates. For a descending ORDER BY Spark negates the
-- offset before adding it, and on Scala 2.13 that rounds a decimal to 34
-- significant digits, so a 35-digit offset of 1e34 + 1 becomes 1e34 and the
-- bound of the row 1e34 + 1 stops at 1, short of 0. On Scala 2.12 the
-- negation is exact. A negative literal such as -N FOLLOWING is parsed
-- exactly, and an ascending ORDER BY adds it unchanged. Either way the native
-- frame must use the offset Spark uses, over rows at the boundary where the
-- two differ.
-- ============================================================

statement
CREATE TABLE dec_negated_35_0(d decimal(35, 0)) USING parquet

statement
INSERT INTO dec_negated_35_0
SELECT CAST(s AS decimal(35, 0)) FROM VALUES
  ('10000000000000000000000000000000001'), ('1'), ('0'), (NULL) AS v(s)

statement
CREATE TABLE dec_negated_35_34(d decimal(35, 34)) USING parquet

statement
INSERT INTO dec_negated_35_34
SELECT CAST(s AS decimal(35, 34)) FROM VALUES
  ('1.0000000000000000000000000000000001'), ('0.0000000000000000000000000000000001'), ('0'),
  (NULL) AS v(s)

query
SELECT d,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND 10000000000000000000000000000000001 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN 10000000000000000000000000000000001 PRECEDING AND CURRENT ROW) AS c1,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN -10000000000000000000000000000000001 FOLLOWING AND CURRENT ROW) AS c2,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND -10000000000000000000000000000000001 PRECEDING) AS c3
FROM dec_negated_35_0

query
SELECT d,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND 10000000000000000000000000000000001 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN 10000000000000000000000000000000001 PRECEDING AND CURRENT ROW) AS c1,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN -10000000000000000000000000000000001 FOLLOWING AND CURRENT ROW) AS c2,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND -10000000000000000000000000000000001 PRECEDING) AS c3
FROM dec_negated_35_0

query
SELECT d,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 10000000000000000000000000000000001 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN 10000000000000000000000000000000001 PRECEDING AND CURRENT ROW) AS c1,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN -10000000000000000000000000000000001 FOLLOWING AND CURRENT ROW) AS c2,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND -10000000000000000000000000000000001 PRECEDING) AS c3
FROM dec_negated_35_0

query
SELECT d,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND 1.0000000000000000000000000000000001 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN 1.0000000000000000000000000000000001 PRECEDING AND CURRENT ROW) AS c1,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN -1.0000000000000000000000000000000001 FOLLOWING AND CURRENT ROW) AS c2,
  COUNT(*) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND -1.0000000000000000000000000000000001 PRECEDING) AS c3
FROM dec_negated_35_34

query
SELECT d,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND 1.0000000000000000000000000000000001 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN 1.0000000000000000000000000000000001 PRECEDING AND CURRENT ROW) AS c1,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN -1.0000000000000000000000000000000001 FOLLOWING AND CURRENT ROW) AS c2,
  COUNT(*) OVER (ORDER BY d DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND -1.0000000000000000000000000000000001 PRECEDING) AS c3
FROM dec_negated_35_34

query
SELECT d,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 1.0000000000000000000000000000000001 FOLLOWING) AS c0,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN 1.0000000000000000000000000000000001 PRECEDING AND CURRENT ROW) AS c1,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN -1.0000000000000000000000000000000001 FOLLOWING AND CURRENT ROW) AS c2,
  COUNT(*) OVER (ORDER BY d DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND -1.0000000000000000000000000000000001 PRECEDING) AS c3
FROM dec_negated_35_34
