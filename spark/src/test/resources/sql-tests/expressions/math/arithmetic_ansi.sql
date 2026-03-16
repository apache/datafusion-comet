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
-- Integer division by zero
-- ============================================================================

-- column / 0 should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT a / b FROM ansi_div_zero

-- column div 0 (integral division) should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT a div b FROM ansi_div_zero

-- column % 0 (remainder) should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT a % b FROM ansi_div_zero

-- literal / 0 should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT 1 / 0

-- literal div 0 should throw
query expect_error(DIVIDE_BY_ZERO)
SELECT 1 div 0

-- literal % 0 should throw
query expect_error(DIVIDE_BY_ZERO)
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
query expect_error(DIVIDE_BY_ZERO)
SELECT c % d FROM ansi_div_zero

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
