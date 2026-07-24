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

-- pmod in legacy (non-ANSI) mode: a zero divisor returns NULL.
-- Config: spark.sql.ansi.enabled=false

statement
CREATE TABLE test_pmod(a int, b int) USING parquet

statement
INSERT INTO test_pmod VALUES
  (7, 3),
  (-7, 3),
  (7, -3),
  (-7, -3),
  (0, 5),
  (5, 0),
  (5, NULL),
  (NULL, 5),
  (-2147483648, 3)

-- column arguments, including negative operands and a zero divisor (NULL in legacy mode)
query
SELECT a, b, pmod(a, b) FROM test_pmod

-- tinyint / smallint / bigint
statement
CREATE TABLE test_pmod_int_types(t tinyint, s smallint, l bigint) USING parquet

statement
INSERT INTO test_pmod_int_types VALUES
  (-7, -7, -7),
  (7, 7, 7),
  (-1, -1, -1),
  (0, 0, 0)

query
SELECT
  pmod(t, cast(3 as tinyint)),
  pmod(s, cast(3 as smallint)),
  pmod(l, cast(3 as bigint))
FROM test_pmod_int_types

-- float and double
statement
CREATE TABLE test_pmod_fp(f float, d double) USING parquet

statement
INSERT INTO test_pmod_fp VALUES
  (-7.5, -7.5),
  (7.5, 7.5),
  (10.5, 10.5),
  (0.0, 0.0)

query
SELECT pmod(f, cast(3.0 as float)), pmod(d, 3.0D) FROM test_pmod_fp

-- floating-point special values: NaN pmod x = NaN, Inf pmod x = NaN, x pmod Inf = x
query
SELECT
  pmod(cast('NaN' as double), 3.0D),
  pmod(cast('Infinity' as double), 3.0D),
  pmod(5.0D, cast('Infinity' as double))

-- literal arguments (constant folding is disabled by the test suite)
query
SELECT pmod(-7, 3), pmod(7, -3), pmod(-7, -3), pmod(5, 0), pmod(0, 5)

-- precision and boundary cases exercised by Spark's own ArithmeticExpressionSuite
query
SELECT pmod(7.2D, 4.1D), pmod(2L, 9223372036854775807L), pmod(-9223372036854775808L, 3L)

-- decimal, including negative operands and a zero divisor (NULL in legacy mode)
statement
CREATE TABLE test_pmod_dec(a decimal(10,2), b decimal(10,2)) USING parquet

statement
INSERT INTO test_pmod_dec VALUES
  (7.5, 3.0),
  (-7.5, 3.0),
  (7.5, -3.0),
  (-7.5, -3.0),
  (0.7, 0.2),
  (5.0, 0.0),
  (5.0, NULL),
  (NULL, 3.0)

query
SELECT a, b, pmod(a, b) FROM test_pmod_dec

-- decimal literals with differing precision and scale (exercises scale coercion)
query
SELECT
  pmod(cast(0.7 as decimal(2,1)), cast(0.2 as decimal(2,1))),
  pmod(cast(7.25 as decimal(5,2)), cast(2.5 as decimal(3,1))),
  pmod(cast(-7.25 as decimal(5,2)), cast(2.5 as decimal(3,1)))

-- wide decimals that exceed Decimal128 precision for the intermediate, exercising the Decimal256
-- promotion path
query
SELECT pmod(cast(1.5 as decimal(38,30)), cast(0.4 as decimal(38,20)))
