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

-- WindowGroupLimit tests with ROW_NUMBER.
-- Spark's InferWindowGroupLimit optimizer rule (SPARK-37099, since 3.5.0) inserts a
-- WindowGroupLimitExec below Window when a rank-like output is filtered by <=, <, or =
-- against an integer literal. Threshold is toggled via ConfigMatrix to exercise both the
-- optimized (WGL-inserted) and the naive (Window + Filter) plan shapes.
--
-- Comet routes the ROW_NUMBER + non-empty PARTITION BY pushdown case to DataFusion's
-- PartitionedTopKExec via `CometWindowGroupLimitExec`. Every pushdown-eligible query below
-- runs natively; queries where Spark's optimizer converts to a plain Limit (global top-K)
-- or skips WGL pushdown still exercise the ORDER BY + Filter path without a fallback.
--
-- Tie-breaking note: ROW_NUMBER is non-deterministic when ORDER BY has duplicate values.
-- Spark's stream-based SimpleLimitIterator preserves input order; DataFusion's heap-based
-- PartitionedTopKExec does not. Every ORDER BY below therefore includes `hire_yr` as a
-- secondary key so the tests exercise WGL without hitting tie-breaking non-determinism.

-- MinSparkVersion: 3.5
-- ConfigMatrix: spark.sql.optimizer.windowGroupLimitThreshold=-1,1000
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_wgl_rn(dept string, salary int, hire_yr int) USING parquet

statement
INSERT INTO test_wgl_rn VALUES
  ('eng',   100,  2020),
  ('eng',   200,  2019),
  ('eng',   200,  2021),
  ('eng',   50,   2022),
  ('eng',   NULL, 2018),
  ('sales', 90,   2020),
  ('sales', 90,   2021),
  ('sales', 300,  2015),
  ('hr',    75,   2023),
  (NULL,    500,  2010),
  (NULL,    NULL, NULL)

-- Top-1 per partition, DESC.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC NULLS LAST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE rn = 1 ORDER BY dept NULLS LAST

-- Top-3 per partition with `<= k`.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC NULLS LAST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE rn <= 3 ORDER BY dept NULLS LAST, rn

-- `< k` form (Spark decrements to `<= k-1`).
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC NULLS LAST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE rn < 3 ORDER BY dept NULLS LAST, rn

-- Literal on the left of the comparison.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC NULLS LAST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE 3 >= rn ORDER BY dept NULLS LAST, rn

-- Limit larger than any partition.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC NULLS LAST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE rn <= 100 ORDER BY dept NULLS LAST, rn

-- Limit = 0 collapses to an empty relation via Spark's InferWindowGroupLimit rule under
-- threshold=1000, and Spark rewrites that to a LocalTableScan (which Comet does not
-- natively support). Under threshold=-1 the query runs via Window+Filter and returns
-- empty. spark_answer_only tolerates both plans.
query spark_answer_only
SELECT dept, salary FROM (
  SELECT dept, salary,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
  FROM test_wgl_rn
) t WHERE rn = 0

-- ASC with NULLS FIRST.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary ASC NULLS FIRST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE rn <= 2 ORDER BY dept NULLS LAST, rn

-- ASC with NULLS LAST.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary ASC NULLS LAST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE rn <= 2 ORDER BY dept NULLS LAST, rn

-- Multi-column ORDER BY (asc, desc). Already deterministic via the two-key ordering.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (
           PARTITION BY dept
           ORDER BY salary DESC NULLS LAST, hire_yr ASC NULLS FIRST
         ) AS rn
  FROM test_wgl_rn
) t WHERE rn <= 2 ORDER BY dept NULLS LAST, rn

-- Multi-column PARTITION BY. Each (dept, hire_yr) pair is unique, so no ties.
query
SELECT dept, hire_yr, salary FROM (
  SELECT dept, hire_yr, salary,
         ROW_NUMBER() OVER (PARTITION BY dept, hire_yr ORDER BY salary DESC NULLS LAST) AS rn
  FROM test_wgl_rn
) t WHERE rn = 1 ORDER BY dept NULLS LAST, hire_yr NULLS LAST

-- No PARTITION BY (global top-K). Spark converts this to a plain Limit for ROW_NUMBER
-- when limit < topKSortFallbackThreshold, so no WGL should appear.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (ORDER BY salary DESC NULLS LAST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE rn <= 3 ORDER BY rn

-- Extra AND predicate on a non-rank column.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC NULLS LAST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE rn <= 2 AND hire_yr >= 2019 ORDER BY dept NULLS LAST, rn

-- Outer LIMIT after WGL pushdown.
query
SELECT dept, salary, hire_yr FROM (
  SELECT dept, salary, hire_yr,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC NULLS LAST, hire_yr ASC NULLS FIRST) AS rn
  FROM test_wgl_rn
) t WHERE rn <= 2 ORDER BY dept NULLS LAST, rn LIMIT 3
