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

-- Tests for CartesianProductExec, which Spark plans for a cross join with no equi key when
-- nothing is broadcast. Comet runs it natively only without a condition and when each input
-- reports exactly one partition. A non-bucketed Parquet scan reports UnknownPartitioning(0), so
-- each side is coalesced to one partition. Partition coalescing is off so AQE cannot merge the
-- REPARTITION(2) case. cartesian_product_aqe.sql covers AQE splitting a REBALANCE(1) input.

-- Config: spark.comet.exec.cartesianProduct.enabled=true
-- Config: spark.sql.autoBroadcastJoinThreshold=-1
-- Config: spark.sql.adaptive.autoBroadcastJoinThreshold=-1
-- Config: spark.sql.adaptive.coalescePartitions.enabled=false
-- ConfigMatrix: spark.sql.adaptive.enabled=true,false

statement
CREATE TABLE cp_left(k int, v int) USING parquet

statement
INSERT INTO cp_left VALUES (0, 10), (NULL, 11), (2, NULL), (NULL, NULL)

statement
CREATE TABLE cp_right(k int, v int) USING parquet

statement
INSERT INTO cp_right VALUES (0, 100), (NULL, NULL)

statement
CREATE TABLE cp_empty(k int, v int) USING parquet

statement
CREATE TABLE cp_large(k int, v int) USING parquet

-- More rows than the default batch size of 8192.
statement
INSERT INTO cp_large SELECT id, id FROM range(10000)

-- CrossJoinExec repeats each left row through ScalarValue, so the left side covers every
-- type with NULLs, nested NULLs, NaN and both signed zeros.
statement
CREATE TABLE cp_types(s string, d decimal(10, 2), ts timestamp, dt date, b binary, bo boolean,
  f double, a array<int>, st struct<x: int, y: string>, m map<string, int>) USING parquet

statement
INSERT INTO cp_types VALUES
  ('a', 1.25, TIMESTAMP '2024-01-01 12:34:56', DATE '2024-01-01', X'0102', true, 1.5,
    array(1, NULL), named_struct('x', 1, 'y', 'p'), map('k', 1)),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  ('', -1.25, TIMESTAMP '1969-12-31 23:59:59', DATE '1969-12-31', X'', false,
    cast('NaN' AS double), array(), named_struct('x', NULL, 'y', NULL), map('k', NULL)),
  ('b', 0.00, TIMESTAMP '1970-01-01 00:00:00', DATE '1970-01-01', X'FF', true,
    cast('-0.0' AS double), array(NULL), named_struct('x', 2, 'y', NULL), map()),
  ('c', 99999999.99, TIMESTAMP '2038-01-19 03:14:07', DATE '9999-12-31', X'00', false,
    0.0, array(2, 3), NULL, NULL)

-- Native, every type on the left side
query
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_types) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_right) r

-- Native, with NULLs on both sides
query
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_left) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_right) r

-- Native, empty left side (the side CrossJoinExec buffers)
query
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_empty) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_right) r

-- Native, empty right side
query
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_left) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_empty) r

-- Native, right side spanning several batches
query
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_left) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_large) r

-- A join condition falls back to Spark
query expect_fallback(CartesianProduct with a join condition is not supported natively)
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_left) l
JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_right) r ON l.v > r.v

-- A multi-partition input falls back to Spark
query expect_fallback(Native CartesianProduct requires single-partition inputs)
SELECT * FROM (SELECT /*+ REPARTITION(2) */ * FROM cp_left) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_right) r
