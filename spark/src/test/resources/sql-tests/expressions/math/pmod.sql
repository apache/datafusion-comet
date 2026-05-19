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

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_pmod(a int, b int, la long, lb long, fa float, fb float, da double, db double) USING parquet

statement
INSERT INTO test_pmod VALUES (10, 3, 10, 3, 10.5, 3.0, 10.5, 3.0), (-10, 3, -10, 3, -10.5, 3.0, -10.5, 3.0), (10, -3, 10, -3, 10.5, -3.0, 10.5, -3.0), (-10, -3, -10, -3, -10.5, -3.0, -10.5, -3.0), (0, 3, 0, 3, 0.0, 3.0, 0.0, 3.0), (NULL, 3, NULL, 3, NULL, 3.0, NULL, 3.0), (10, NULL, 10, NULL, 10.5, cast(NULL as float), 10.5, cast(NULL as double))

-- column arguments: int
query
SELECT pmod(a, b) FROM test_pmod

-- column arguments: long
query
SELECT pmod(la, lb) FROM test_pmod

-- column arguments: float
query
SELECT pmod(fa, fb) FROM test_pmod

-- column arguments: double
query
SELECT pmod(da, db) FROM test_pmod

-- literal arguments
query
SELECT pmod(10, 3), pmod(-10, 3), pmod(10, -3), pmod(-10, -3)

-- literal with NULL
query
SELECT pmod(NULL, 3), pmod(10, NULL), pmod(NULL, NULL)

-- division by zero returns NULL in non-ANSI mode
query
SELECT pmod(10, 0), pmod(-10, 0)

-- decimal type
statement
CREATE TABLE test_pmod_dec(a decimal(10,2), b decimal(10,2)) USING parquet

statement
INSERT INTO test_pmod_dec VALUES (10.50, 3.00), (-10.50, 3.00), (10.50, -3.00), (-10.50, -3.00), (NULL, 3.00)

query
SELECT pmod(a, b) FROM test_pmod_dec

-- short type
statement
CREATE TABLE test_pmod_short(a short, b short) USING parquet

statement
INSERT INTO test_pmod_short VALUES (10, 3), (-10, 3), (10, -3), (-10, -3)

query
SELECT pmod(a, b) FROM test_pmod_short

-- byte type
statement
CREATE TABLE test_pmod_byte(a byte, b byte) USING parquet

statement
INSERT INTO test_pmod_byte VALUES (10, 3), (-10, 3), (10, -3), (-10, -3)

query
SELECT pmod(a, b) FROM test_pmod_byte

-- boundary values
query
SELECT pmod(2L, 9223372036854775807L), pmod(2147483647, -2147483648), pmod(-2147483648, -1)

-- NaN and Infinity for double
query
SELECT pmod(cast('NaN' as double), 3.0), pmod(3.0, cast('NaN' as double)), pmod(cast('Infinity' as double), 3.0), pmod(3.0, cast('Infinity' as double)), pmod(cast('-Infinity' as double), 3.0)

-- NaN and Infinity for float
query
SELECT pmod(cast('NaN' as float), cast(3.0 as float)), pmod(cast(3.0 as float), cast('NaN' as float)), pmod(cast('Infinity' as float), cast(3.0 as float)), pmod(cast(3.0 as float), cast('Infinity' as float))

-- negative zero: Spark preserves -0.0 for the non-negative branch
query
SELECT pmod(cast('-0.0' as double), 3.0), pmod(cast('-0.0' as float), cast(3.0 as float))

-- high-precision decimal (exercises Decimal256 promotion path)
statement
CREATE TABLE test_pmod_dec256(a decimal(38,18), b decimal(38,18)) USING parquet

statement
INSERT INTO test_pmod_dec256 VALUES (12345678901234567890.123456789012345678, 3.000000000000000000), (-12345678901234567890.123456789012345678, 3.000000000000000000)

query
SELECT pmod(a, b) FROM test_pmod_dec256
