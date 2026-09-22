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

-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.ansi.enabled=false

-- ROWS ... N PRECEDING is covered through the DataFrame API in CometWindowExecSuite
-- so this fixture can keep the harness's ConstantFolding exclusion.

statement
CREATE TABLE sliding_integer_sum(g INT, id INT, v BIGINT) USING parquet

statement
INSERT INTO sliding_integer_sum VALUES
  (1, 1, 9223372036854775807), (1, 2, 1), (1, 3, -1), (1, 4, NULL), (1, 5, NULL),
  (2, 1, -9223372036854775808), (2, 2, -1), (2, 3, 1), (2, 4, NULL), (2, 5, NULL),
  (3, 1, NULL), (3, 2, NULL), (3, 3, NULL)

-- TRY mode must fall back even when ANSI is disabled. Cover positive/negative
-- overflow, recovery after it leaves the frame, all-NULL and empty frames.
query expect_fallback(ANSI/TRY SUM on integral types with a sliding window frame is not supported)
SELECT g, id, try_sum(v) OVER (
  PARTITION BY g ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)
FROM sliding_integer_sum

query expect_fallback(ANSI/TRY SUM on integral types with a sliding window frame is not supported)
SELECT g, id, try_sum(v) OVER (
  PARTITION BY g ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM sliding_integer_sum

query expect_fallback(ANSI/TRY SUM on integral types with a sliding window frame is not supported)
SELECT g, id, try_sum(v) OVER (
  PARTITION BY g ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
FROM sliding_integer_sum

-- Legacy sliding sums still run natively, including wrapping overflow.
query
SELECT g, id,
  sum(v) OVER (PARTITION BY g ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)
FROM sliding_integer_sum

-- Ever-expanding TRY sums retain native overflow-to-NULL behavior.
query
SELECT g, id, try_sum(v) OVER (
  PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM sliding_integer_sum

statement
SET spark.sql.ansi.enabled=true

query expect_error(ARITHMETIC_OVERFLOW)
SELECT sum(v) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)
FROM sliding_integer_sum WHERE g = 2

-- Native expanding sums remain enabled in ANSI and TRY mode.
query
SELECT g, id,
  sum(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
  try_sum(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM sliding_integer_sum WHERE id > 1

query
SELECT g, id, try_sum(v) OVER (
  PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM sliding_integer_sum

query expect_error(ARITHMETIC_OVERFLOW)
SELECT sum(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM sliding_integer_sum WHERE g = 1
