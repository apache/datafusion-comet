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

-- Decimal division (/) and integral division (div) in ANSI mode.
-- Divide-by-zero and overflow must throw; try_divide still returns NULL.
-- See decimal_div.sql for legacy-mode and try_divide tests.

-- Config: spark.sql.ansi.enabled=true

-- ============================================================================
-- Setup
-- ============================================================================

statement
CREATE TABLE test_ansi_decimal(a decimal(18,6), b decimal(18,6)) USING parquet

statement
INSERT INTO test_ansi_decimal VALUES
  (10.000000,  3.000000),
  (7.500000,   2.500000),
  (-10.000000, 3.000000),
  (10.000000, -3.000000),
  (0.000000,   5.000000),
  (NULL,       3.000000),
  (10.000000,  NULL)

statement
CREATE TABLE test_ansi_zero(a decimal(18,6)) USING parquet

statement
INSERT INTO test_ansi_zero VALUES (10.000000), (-5.000000), (0.000000), (NULL)

-- ============================================================================
-- Normal division works in ANSI mode (no zero divisor, no overflow)
-- ============================================================================

query
SELECT a / b FROM test_ansi_decimal

query
SELECT a div b FROM test_ansi_decimal

-- ============================================================================
-- ANSI mode: decimal / by zero throws DIVIDE_BY_ZERO
-- ============================================================================

-- column / zero column
query expect_error(DIVIDE_BY_ZERO)
SELECT a / cast(0.000000 as decimal(18,6)) FROM test_ansi_zero

-- literal / zero literal
query expect_error(DIVIDE_BY_ZERO)
SELECT cast(10.0 as decimal(18,6)) / cast(0.000000 as decimal(18,6))

-- ============================================================================
-- ANSI mode: decimal div by zero throws DIVIDE_BY_ZERO
-- ============================================================================

-- column div zero column
query expect_error(DIVIDE_BY_ZERO)
SELECT a div cast(0.000000 as decimal(18,6)) FROM test_ansi_zero

-- literal div zero literal
query expect_error(DIVIDE_BY_ZERO)
SELECT cast(10 as decimal(18,0)) div cast(0.000000 as decimal(18,6))

-- ============================================================================
-- TRY mode: try_divide returns NULL even when ANSI is enabled globally
-- ============================================================================

-- try_divide by zero -> null (TRY semantics override ANSI)
query
SELECT try_divide(a, cast(0.000000 as decimal(18,6))) FROM test_ansi_zero

-- try_divide with normal values
query
SELECT try_divide(a, b) FROM test_ansi_decimal

-- try_divide NULL inputs
query
SELECT try_divide(NULL, cast(3.000000 as decimal(18,6))),
       try_divide(cast(10.000000 as decimal(18,6)), NULL)

-- ============================================================================
-- ANSI mode: decimal div overflow throws NUMERIC_VALUE_OUT_OF_RANGE
-- All values produce a quotient > Decimal(38,0).max so every row overflows.
-- ============================================================================

statement
CREATE TABLE test_ansi_overflow(a decimal(38,0), b decimal(2,2)) USING parquet

statement
INSERT INTO test_ansi_overflow VALUES
  (-62672277069777110394022909049981876593, -0.40),
  ( 54400354300704342908577384819323710194,  0.18)

query expect_error(NUMERIC_VALUE_OUT_OF_RANGE)
SELECT a div b FROM test_ansi_overflow

-- ============================================================================
-- ANSI mode: decimal / overflow produces null (Divide uses nullable output
-- type for overflow; only ANSI integral-divide uses CheckOverflow that throws)
-- ============================================================================

statement
CREATE TABLE test_ansi_div_overflow(a decimal(38,0)) USING parquet

statement
INSERT INTO test_ansi_div_overflow VALUES
  (99999999999999999999999999999999999999)

-- Dividing max decimal(38,0) by 0.01 overflows the output type.
-- In ANSI mode Divide still returns null for overflow (not an error),
-- matching Spark's behaviour where Divide.failOnError governs only
-- divide-by-zero, not precision overflow.
query
SELECT a / cast(0.01 as decimal(10,2)) FROM test_ansi_div_overflow
