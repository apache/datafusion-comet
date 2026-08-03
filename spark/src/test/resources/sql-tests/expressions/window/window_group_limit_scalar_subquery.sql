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

-- Repro for SPARK-46526-shaped correlated scalar subquery with `ORDER BY colX
-- LIMIT 1`. Spark decorrelates by wrapping the subquery in a rank window; if
-- the ORDER BY column collapses into the correlation (e.g. ORDER BY t2c when
-- the correlation predicate is `WHERE t2c = t1c`), the resulting Window may
-- have an EMPTY orderSpec but non-empty partitionSpec. Comet's serde must not
-- hand DataFusion's `PartitionedTopKExec` an ordering with only partition
-- keys -- that panics at execute time.
--
-- Spark 3.5 rejects this correlation form at analysis
-- (UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED);
-- the decorrelation that produces the WGL landed in Spark 4.0.

-- MinSparkVersion: 4.0

statement
CREATE TABLE t1(t1a string, t1b smallint, t1c int, t1d int) USING parquet

statement
INSERT INTO t1 VALUES
  ('t1a-1', cast(1 AS smallint), 1,   10),
  ('t1a-2', cast(2 AS smallint), 2,   5),
  ('t1a-3', cast(3 AS smallint), 3,   NULL),
  ('t1a-4', cast(4 AS smallint), NULL, 20)

statement
CREATE TABLE t2(t2a string, t2b smallint, t2c int, t2d int) USING parquet

statement
INSERT INTO t2 VALUES
  ('t2a-1', cast(1 AS smallint), 1, 100),
  ('t2a-1b', cast(11 AS smallint), 1, 200),
  ('t2a-2', cast(2 AS smallint), 2, 300),
  ('t2a-3', cast(3 AS smallint), 3, 400)

-- Original failing query.
query
SELECT t1a, t1b
FROM   t1
WHERE  t1c = (SELECT t2c
              FROM   t2
              WHERE  t2c = t1c
              ORDER BY t2c LIMIT 1)
ORDER BY t1a

-- Same shape but with a non-degenerate ORDER BY column (t2d instead of t2c).
query
SELECT t1a, t1b
FROM   t1
WHERE  t1c = (SELECT t2c
              FROM   t2
              WHERE  t2c = t1c
              ORDER BY t2d LIMIT 1)
ORDER BY t1a
