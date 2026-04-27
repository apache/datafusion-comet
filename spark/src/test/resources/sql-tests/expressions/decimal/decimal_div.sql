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

-- Decimal division (/) and integral division (div) in legacy (non-ANSI) mode.
-- Also covers try_divide (TRY mode semantics).
-- See decimal_div_ansi.sql for ANSI mode tests.

-- ConfigMatrix: parquet.enable.dictionary=false,true

-- ============================================================================
-- Basic divide and integral divide
-- ============================================================================

statement
CREATE TABLE test_decimal_div(a decimal(18,6), b decimal(18,6)) USING parquet

statement
INSERT INTO test_decimal_div VALUES
  (10.000000,  3.000000),
  (7.500000,   2.500000),
  (-10.000000, 3.000000),
  (10.000000, -3.000000),
  (-10.000000,-3.000000),
  (0.000000,   5.000000),
  (1.000000,   3.000000),
  (NULL,       3.000000),
  (10.000000,  NULL)

-- column / column
query
SELECT a / b FROM test_decimal_div

-- column div column (integral division: drops fractional part)
query
SELECT a div b FROM test_decimal_div

-- ============================================================================
-- Divide by zero: legacy mode returns NULL, not an error
-- ============================================================================

statement
CREATE TABLE test_decimal_div_zero(a decimal(18,6)) USING parquet

statement
INSERT INTO test_decimal_div_zero VALUES
  (10.000000),
  (-5.000000),
  (0.000000),
  (NULL)

-- a / 0 returns null in legacy mode
query
SELECT a / cast(0 as decimal(18,6)) FROM test_decimal_div_zero

-- a div 0 returns null in legacy mode
query
SELECT a div cast(0 as decimal(18,6)) FROM test_decimal_div_zero

-- ============================================================================
-- TRY mode: try_divide always returns NULL on zero divisor
-- ============================================================================

-- try_divide by zero returns null even when ANSI mode is on externally
query
SELECT try_divide(a, cast(0.000000 as decimal(18,6))) FROM test_decimal_div_zero

-- try_divide with non-zero divisor returns the quotient
query
SELECT try_divide(a, b) FROM test_decimal_div

-- try_divide literal decimal by zero
-- IgnoreFromSparkVersion: 4.1 https://github.com/apache/datafusion-comet/issues/4098
query
SELECT try_divide(cast(10.5 as decimal(10,2)), cast(0.0 as decimal(10,2)))

-- try_divide with NULL inputs
query
SELECT try_divide(NULL, cast(3.0 as decimal(10,2))),
       try_divide(cast(10.0 as decimal(10,2)), NULL)

-- ============================================================================
-- Overflow in legacy mode produces NULL
-- ============================================================================

statement
CREATE TABLE test_decimal_overflow(a decimal(38,0)) USING parquet

statement
INSERT INTO test_decimal_overflow VALUES
  (99999999999999999999999999999999999999),
  (-99999999999999999999999999999999999999)

-- Dividing max decimal(38,0) by a small fraction overflows the result type -> null
query
SELECT a / cast(0.01 as decimal(10,2)) FROM test_decimal_overflow

-- Integral divide overflow -> null
query
SELECT a div cast(0.40 as decimal(2,2)) FROM test_decimal_overflow

-- ============================================================================
-- Literal arguments (constant folding is disabled by the test runner)
-- ============================================================================

-- literal / literal
query
SELECT cast(10.5 as decimal(10,2)) / cast(3.5 as decimal(10,2))

-- literal div literal
query
SELECT cast(10 as decimal(10,0)) div cast(3 as decimal(10,0))

-- literal / zero
query
SELECT cast(7.0 as decimal(10,2)) / cast(0.0 as decimal(10,2))

-- literal div zero
query
SELECT cast(7 as decimal(10,0)) div cast(0 as decimal(10,0))

-- NULL literal
query
SELECT cast(NULL as decimal(10,2)) / cast(3.0 as decimal(10,2)),
       cast(5.0 as decimal(10,2)) / cast(NULL as decimal(10,2))

-- ============================================================================
-- Mixed precision and scale
-- ============================================================================

statement
CREATE TABLE test_decimal_mixed(a decimal(18,6), b decimal(10,2)) USING parquet

statement
INSERT INTO test_decimal_mixed VALUES
  (123456.789012, 99.99),
  (0.000001, 1.00),
  (-123456.789012, 0.01),
  (NULL, NULL)

-- spark_answer_only: mixed-precision division can trigger the precision-loss path in Spark's
-- decimal arithmetic which Comet does not yet handle natively; see
-- https://github.com/apache/datafusion-comet/issues/1526
query spark_answer_only
SELECT a / b FROM test_decimal_mixed

query
SELECT a div b FROM test_decimal_mixed

-- ============================================================================
-- Various precision/scale combinations
-- ============================================================================

statement
CREATE TABLE test_decimal_prec(
  a5_2 decimal(5,2),
  b5_2 decimal(5,2),
  a38_4 decimal(38,4),
  b38_4 decimal(38,4)
) USING parquet

statement
INSERT INTO test_decimal_prec VALUES
  (10.50, 3.25, 9999999999999999.1234, 3.0001),
  (-10.50, 3.25, -9999999999999999.1234, 3.0001),
  (0.00, 1.00, 0.0000, 1.0000),
  (NULL, NULL, NULL, NULL)

query
SELECT a5_2 / b5_2 FROM test_decimal_prec

query
SELECT a5_2 div b5_2 FROM test_decimal_prec

-- spark_answer_only: decimal(38,4) / decimal(38,4) produces a result type that exceeds
-- Decimal(38,x) precision, triggering a precision-loss path Comet does not yet handle natively;
-- see https://github.com/apache/datafusion-comet/issues/1526
query spark_answer_only
SELECT a38_4 / b38_4 FROM test_decimal_prec

query
SELECT a38_4 div b38_4 FROM test_decimal_prec
