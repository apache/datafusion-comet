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

-- pmod in ANSI mode: a zero divisor raises an error instead of returning NULL.
-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_pmod_ansi(a int, b int) USING parquet

statement
INSERT INTO test_pmod_ansi VALUES (7, 3), (-7, 3), (7, -3), (-7, -3)

-- Sentinel query: non-zero divisors must compute natively and match Spark. This guards against a
-- vacuous pass where Comet falls back to Spark for the error cases below.
query
SELECT a, b, pmod(a, b) FROM test_pmod_ansi

-- column zero divisor throws. Spark 4.0 raises DIVIDE_BY_ZERO, Spark 4.1 raises REMAINDER_BY_ZERO;
-- match the common substring.
query expect_error(BY_ZERO)
SELECT pmod(a, 0) FROM test_pmod_ansi

-- literal zero divisor throws (constant folding is disabled by the test suite)
query expect_error(BY_ZERO)
SELECT pmod(7, 0)

-- decimal zero divisor also throws in ANSI mode
query expect_error(BY_ZERO)
SELECT pmod(cast(5.0 as decimal(10,2)), cast(0.0 as decimal(10,2)))
