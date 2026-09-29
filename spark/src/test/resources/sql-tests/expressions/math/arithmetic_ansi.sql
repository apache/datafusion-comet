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

-- ANSI mode arithmetic tests
-- Tests that ANSI mode throws exceptions for overflow and division by zero

-- Config: spark.sql.ansi.enabled=true

-- ============================================================================
-- Test data setup for integer overflow
-- ============================================================================

statement
CREATE TABLE ansi_int_overflow(a int, b int) USING parquet

statement
INSERT INTO ansi_int_overflow VALUES (2147483647, 1), (-2147483648, 1), (-2147483648, -1)

statement
CREATE TABLE ansi_long_overflow(a long, b long) USING parquet

statement
INSERT INTO ansi_long_overflow VALUES (9223372036854775807, 1), (-9223372036854775808, 1), (-9223372036854775808, -1)

statement
CREATE TABLE ansi_div_zero(a int, b int, c long, d long) USING parquet

statement
INSERT INTO ansi_div_zero VALUES (1, 0, 1, 0)

-- ============================================================================
-- Integer addition overflow
-- ============================================================================

-- INT_MAX + 1 should overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT a + b FROM ansi_int_overflow WHERE a = 2147483647

-- literal overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT 2147483647 + 1

-- ============================================================================
-- Integer subtraction overflow
-- ============================================================================

-- INT_MIN - 1 should overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT a - b FROM ansi_int_overflow WHERE a = -2147483648

-- literal overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT -2147483648 - 1

-- ============================================================================
-- Integer multiplication overflow
-- ============================================================================

-- INT_MAX * 2 should overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT a * 2 FROM ansi_int_overflow WHERE a = 2147483647

-- literal overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT 2147483647 * 2

-- ============================================================================
-- Long addition overflow
-- ============================================================================

-- LONG_MAX + 1 should overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT a + b FROM ansi_long_overflow WHERE a = 9223372036854775807

-- ============================================================================
-- Long subtraction overflow
-- ============================================================================

-- LONG_MIN - 1 should overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT a - b FROM ansi_long_overflow WHERE a = -9223372036854775808

-- ============================================================================
-- Long multiplication overflow
-- ============================================================================

-- LONG_MAX * 2 should overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT a * 2 FROM ansi_long_overflow WHERE a = 9223372036854775807

-- ============================================================================
-- Integral divide overflow
-- ============================================================================

-- LONG_MIN div -1 should overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT a div b FROM ansi_long_overflow WHERE a = -9223372036854775808 AND b = -1

-- literal LONG_MIN div -1 should overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT -9223372036854775808L div -1L

-- INT_MIN div -1 does not overflow because the result type is LONG
query
SELECT a div b FROM ansi_int_overflow WHERE a = -2147483648 AND b = -1

-- LONG_MIN div 1 does not overflow
query
SELECT a div b FROM ansi_long_overflow WHERE b = 1

-- Spark only checks integral divide overflow for LONG operands; DECIMAL operands
-- wrap around on the implicit cast to LONG even in ANSI mode
query
SELECT CAST(a AS DECIMAL(19,0)) div CAST(b AS DECIMAL(19,0)) FROM ansi_long_overflow WHERE a = -9223372036854775808 AND b = -1

-- ============================================================================
-- Integer division by zero
-- ============================================================================

-- column / 0 should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT a / b FROM ansi_div_zero

-- column div 0 (integral division) should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT a div b FROM ansi_div_zero

-- column % 0 (remainder) should throw
-- Spark 4.0 raises DIVIDE_BY_ZERO; Spark 4.1 raises REMAINDER_BY_ZERO. Match the common substring.
query expect_error(BY_ZERO)
SELECT a % b FROM ansi_div_zero

-- literal / 0 should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT 1 / 0

-- literal div 0 should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT 1 div 0

-- literal % 0 should throw
query expect_error(BY_ZERO)
SELECT 1 % 0

-- ============================================================================
-- Long division by zero
-- ============================================================================

-- long column / 0 should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT c / d FROM ansi_div_zero

-- long column div 0 should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT c div d FROM ansi_div_zero

-- long column % 0 should throw
query expect_error(BY_ZERO)
SELECT c % d FROM ansi_div_zero

-- ============================================================================
-- Float/Double remainder by zero (issue #5067)
-- Spark 4.1 throws REMAINDER_BY_ZERO for floating-point x % 0 as well.
-- ============================================================================

statement
CREATE TABLE ansi_float_div_zero(a float, b float, c double, d double) USING parquet

statement
INSERT INTO ansi_float_div_zero VALUES (3.0, 0.0, 3.0, 0.0), (1.0, float('-0.0'), 1.0, double('-0.0'))

-- float column % 0 should throw
query expect_error(BY_ZERO)
SELECT a % b FROM ansi_float_div_zero WHERE b = 0.0

-- double column % 0 should throw
query expect_error(BY_ZERO)
SELECT c % d FROM ansi_float_div_zero WHERE d = 0.0

-- The cases below keep a column operand so that ConstantFolding cannot evaluate them
-- during optimization, which means they reach the native remainder at execution time.

-- float column % literal 0.0 should throw
query expect_error(BY_ZERO)
SELECT a % CAST(0.0 AS FLOAT) FROM ansi_float_div_zero

-- double column % literal 0.0 should throw
query expect_error(BY_ZERO)
SELECT c % CAST(0.0 AS DOUBLE) FROM ansi_float_div_zero

-- double column % literal -0.0 should also throw (IEEE 754: -0.0 == 0.0)
query expect_error(BY_ZERO)
SELECT c % double('-0.0') FROM ansi_float_div_zero

-- literal double % double column should throw
query expect_error(BY_ZERO)
SELECT CAST(1.0 AS DOUBLE) % d FROM ansi_float_div_zero

-- a non-zero literal divisor must still produce results rather than throw
query
SELECT c % CAST(2.0 AS DOUBLE) FROM ansi_float_div_zero ORDER BY 1

-- The fully-literal cases below are folded by Spark's ConstantFolding rule before the
-- query reaches Comet, so they assert Spark's own behavior rather than the native path.

-- literal double % 0.0 should throw
query expect_error(BY_ZERO)
SELECT CAST(1.0 AS DOUBLE) % CAST(0.0 AS DOUBLE)

-- literal double % -0.0 should also throw (IEEE 754: -0.0 == 0.0)
query expect_error(BY_ZERO)
SELECT CAST(1.0 AS DOUBLE) % double('-0.0')

-- literal float % 0.0 should throw
query expect_error(BY_ZERO)
SELECT CAST(1.0 AS FLOAT) % CAST(0.0 AS FLOAT)

-- ----------------------------------------------------------------------------
-- Zero divisor paired with special dividends.
-- Spark's DivModLike.eval only inspects the divisor when deciding whether to raise, so
-- NaN, +/-Infinity and 0.0 dividends must all throw rather than yielding NaN. Keeping the
-- dividend as a column puts these out of reach of ConstantFolding.
-- ----------------------------------------------------------------------------

statement
CREATE TABLE ansi_float_special(a double) USING parquet

statement
INSERT INTO ansi_float_special VALUES (double('NaN')), (double('Infinity')), (double('-Infinity')), (0.0)

-- NaN % 0 should throw
query expect_error(BY_ZERO)
SELECT a % CAST(0.0 AS DOUBLE) FROM ansi_float_special WHERE isnan(a)

-- +Infinity % 0 should throw
query expect_error(BY_ZERO)
SELECT a % CAST(0.0 AS DOUBLE) FROM ansi_float_special WHERE a = double('Infinity')

-- -Infinity % 0 should throw
query expect_error(BY_ZERO)
SELECT a % CAST(0.0 AS DOUBLE) FROM ansi_float_special WHERE a = double('-Infinity')

-- 0 % 0 should throw
query expect_error(BY_ZERO)
SELECT a % CAST(0.0 AS DOUBLE) FROM ansi_float_special WHERE a = 0.0

-- the same special dividends with a non-zero divisor must not throw. Compare isnan() rather
-- than the values themselves: NaN % 2.0 and +/-Infinity % 2.0 are NaN, and the sign of a NaN
-- produced by an invalid fmod is platform-dependent.
query
SELECT isnan(a % CAST(2.0 AS DOUBLE)) AS r FROM ansi_float_special ORDER BY r

-- a finite dividend with a non-zero divisor still compares by value
query
SELECT a % CAST(2.0 AS DOUBLE) FROM ansi_float_special WHERE a = 0.0

-- ----------------------------------------------------------------------------
-- Mixed-row batches: some rows have a zero divisor, some do not, and some have a null
-- dividend. Comet raises once per batch, so only a mixed batch checks that the zero
-- divisor is correlated with dividend nullness rather than tested batch-wide.
-- ----------------------------------------------------------------------------

statement
CREATE TABLE ansi_float_mixed(a double, b double) USING parquet

statement
INSERT INTO ansi_float_mixed VALUES (1.0, 2.0), (3.0, 0.0), (NULL, 0.0), (5.0, 1.5), (NULL, 4.0)

-- a non-null dividend meets a zero divisor somewhere in the batch, so this must throw
query expect_error(BY_ZERO)
SELECT a % b FROM ansi_float_mixed

-- the only zero divisor left pairs with a null dividend, so this must return null, not throw
query
SELECT a % b FROM ansi_float_mixed WHERE a IS NULL ORDER BY 1

-- no zero divisors in this batch at all
query
SELECT a % b FROM ansi_float_mixed WHERE b <> 0.0 ORDER BY 1

-- ============================================================================
-- Unary minus overflow
-- ============================================================================

-- negating INT_MIN should overflow (since INT_MAX is 2147483647, -(-2147483648) cannot fit)
query expect_error(ARITHMETIC_OVERFLOW)
SELECT -a FROM ansi_int_overflow WHERE a = -2147483648

-- negating LONG_MIN should overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT -a FROM ansi_long_overflow WHERE a = -9223372036854775808

-- literal negation overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT -(-2147483648)

-- literal long negation overflow
query expect_error(ARITHMETIC_OVERFLOW)
SELECT -(-9223372036854775808L)
