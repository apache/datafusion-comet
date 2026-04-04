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

-- ANSI mode abs function tests
-- Tests that abs throws exceptions for overflow on minimum integer values

-- Config: spark.sql.ansi.enabled=true

-- ============================================================================
-- Test data setup
-- ============================================================================

statement
CREATE TABLE ansi_test_abs_int(v int) USING parquet

statement
INSERT INTO ansi_test_abs_int VALUES (-2147483648)

statement
CREATE TABLE ansi_test_abs_long(v long) USING parquet

statement
INSERT INTO ansi_test_abs_long VALUES (-9223372036854775808)

statement
CREATE TABLE ansi_test_abs_short(v short) USING parquet

statement
INSERT INTO ansi_test_abs_short VALUES (-32768)

statement
CREATE TABLE ansi_test_abs_byte(v tinyint) USING parquet

statement
INSERT INTO ansi_test_abs_byte VALUES (-128)

-- ============================================================================
-- abs(INT_MIN) overflow
-- ============================================================================

-- abs(-2147483648) cannot be represented as int (since INT_MAX = 2147483647)
query expect_error(overflow)
SELECT abs(v) FROM ansi_test_abs_int

-- literal
query expect_error(overflow)
SELECT abs(-2147483648)

-- ============================================================================
-- abs(LONG_MIN) overflow
-- ============================================================================

-- abs(-9223372036854775808) cannot be represented as long
query expect_error(overflow)
SELECT abs(v) FROM ansi_test_abs_long

-- literal
query expect_error(overflow)
SELECT abs(-9223372036854775808L)

-- ============================================================================
-- abs(SHORT_MIN) overflow
-- ============================================================================

-- abs(-32768) cannot be represented as short
query expect_error(overflow)
SELECT abs(v) FROM ansi_test_abs_short

-- literal
query expect_error(overflow)
SELECT abs(cast(-32768 as short))

-- ============================================================================
-- abs(BYTE_MIN) overflow
-- ============================================================================

-- abs(-128) cannot be represented as tinyint
query expect_error(overflow)
SELECT abs(v) FROM ansi_test_abs_byte

-- literal
query expect_error(overflow)
SELECT abs(cast(-128 as tinyint))
