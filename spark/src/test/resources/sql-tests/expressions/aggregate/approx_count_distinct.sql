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

-- approx_count_distinct estimates the number of distinct values using HyperLogLog++.
-- Comet ports Spark's HyperLogLogPlusPlus exactly, so results are compared for equality.

-- ============================================================
-- A table with a range of cardinalities across many magnitudes
-- ============================================================

statement
CREATE TABLE acd_big USING parquet AS
SELECT
  id,
  CAST(id % 10 AS INT) AS c10,
  CAST(id % 500 AS INT) AS c500,
  CAST(id % 5000 AS INT) AS c5000,
  CAST(id % 8 AS INT) AS grp
FROM range(50000)

-- full cardinality (50000 distinct)
query
SELECT approx_count_distinct(id) FROM acd_big

-- a spread of cardinalities exercising linear counting, bias correction, and plain HLL
query
SELECT
  approx_count_distinct(c10),
  approx_count_distinct(c500),
  approx_count_distinct(c5000)
FROM acd_big

-- grouped
query
SELECT grp, approx_count_distinct(id) FROM acd_big GROUP BY grp ORDER BY grp

-- ============================================================
-- relativeSD argument (changes the precision p)
-- ============================================================

query
SELECT approx_count_distinct(id, 0.01) FROM acd_big

query
SELECT approx_count_distinct(id, 0.1) FROM acd_big

query
SELECT approx_count_distinct(c5000, 0.02) FROM acd_big

-- ============================================================
-- Small cardinality and NULL handling across many types
-- ============================================================

statement
CREATE TABLE acd_types(i int, l bigint, s string, f float, d double, dec decimal(10,2), dt date, ts timestamp, b boolean, bin binary, grp string) USING parquet

statement
INSERT INTO acd_types VALUES
  (1, 100, 'a', 1.5, 1.25, 1.10, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', true,  X'CAFE', 'g1'),
  (2, 200, 'b', 2.5, 2.25, 2.20, DATE '2024-02-01', TIMESTAMP '2024-02-01 00:00:00', false, X'BABE', 'g1'),
  (1, 100, 'a', 1.5, 1.25, 1.10, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', true,  X'CAFE', 'g1'),
  (3, 300, 'c', 3.5, 3.25, 3.30, DATE '2024-03-01', TIMESTAMP '2024-03-01 00:00:00', true,  X'FEED', 'g2'),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'g2')

query
SELECT
  approx_count_distinct(i),
  approx_count_distinct(l),
  approx_count_distinct(s),
  approx_count_distinct(f),
  approx_count_distinct(d),
  approx_count_distinct(dec),
  approx_count_distinct(dt),
  approx_count_distinct(ts),
  approx_count_distinct(b),
  approx_count_distinct(bin)
FROM acd_types

query
SELECT grp, approx_count_distinct(i) FROM acd_types GROUP BY grp ORDER BY grp

-- ============================================================
-- Empty table returns 0 (not NULL)
-- ============================================================

statement
CREATE TABLE acd_empty(i int) USING parquet

query
SELECT approx_count_distinct(i) FROM acd_empty

-- all-NULL column returns 0
query
SELECT approx_count_distinct(i) FROM acd_types WHERE i IS NULL

-- ============================================================
-- Decimal precision boundary: <= 18 hashes via the unscaled long (native),
-- > 18 hashes via BigDecimal in Spark, which the native path does not match (fallback)
-- ============================================================

statement
CREATE TABLE acd_dec18(d decimal(18,4)) USING parquet

statement
INSERT INTO acd_dec18
SELECT CAST(id AS decimal(18,4)) + 0.0001 FROM range(20000)

-- decimal precision 18 (the boundary): runs natively and matches Spark
query
SELECT approx_count_distinct(d) FROM acd_dec18

statement
CREATE TABLE acd_dec38(d decimal(38,4)) USING parquet

statement
INSERT INTO acd_dec38
SELECT CAST(id AS decimal(38,4)) + 0.0001 FROM range(20000)

-- decimal precision > 18: Spark hashes via BigDecimal, so Comet must fall back
query expect_fallback(Unsupported input data type)
SELECT approx_count_distinct(d) FROM acd_dec38

-- ============================================================
-- Float/double normalization: -0.0 == 0.0 and all NaN are equal
-- ============================================================

statement
CREATE TABLE acd_float(d double) USING parquet

statement
INSERT INTO acd_float VALUES
  (0.0), (-0.0), (CAST('NaN' AS DOUBLE)), (CAST('NaN' AS DOUBLE)), (1.0)

-- distinct values are {0.0, NaN, 1.0} = 3
query
SELECT approx_count_distinct(d) FROM acd_float

-- ============================================================
-- Combined with other aggregates
-- ============================================================

query
SELECT grp, approx_count_distinct(id), count(*), count(DISTINCT grp)
FROM acd_big GROUP BY grp ORDER BY grp
