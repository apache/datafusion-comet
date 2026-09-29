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

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.batchSize=2

statement
CREATE TABLE test_if(cond boolean, a int, b int) USING parquet

statement
INSERT INTO test_if VALUES (true, 1, 2), (false, 1, 2), (NULL, 1, 2), (true, NULL, 2), (false, 1, NULL)

query
SELECT IF(cond, a, b) FROM test_if

query
SELECT IF(a > 0, 'positive', 'non-positive') FROM test_if

-- literal arguments
query
SELECT IF(true, 1, 2), IF(false, 1, 2), IF(NULL, 1, 2)

-- Both column/literal directions, NULL conditions and unselected NULL values.
query expect_native(if)
SELECT IF(cond, a, 9), IF(cond, 9, a) FROM test_if

-- NULL literals and computed branches keep the existing short-circuit paths.
query expect_native(if)
SELECT IF(cond, a, NULL), IF(cond, NULL, b), IF(cond, a + 1, b - 1) FROM test_if

-- Spark inserts branch casts when the result types differ.
query expect_native(if)
SELECT IF(cond, a, 9L), IF(cond, 9L, a) FROM test_if

statement
CREATE TABLE test_if_values(cond boolean, d double, s string) USING parquet

statement
INSERT INTO test_if_values VALUES (true, cast('NaN' AS DOUBLE), ''), (false, -0.0D, '中文'), (NULL, cast('Infinity' AS DOUBLE), NULL), (true, NULL, 'longer value'), (false, 7.0D, '')

query expect_native(if)
SELECT IF(cond, d, 0D), IF(cond, 0D, d), IF(isnan(d), 0D, d) FROM test_if_values

query expect_native(if)
SELECT IF(cond, s, 'replacement'), IF(cond, '中文', s) FROM test_if_values

statement
CREATE TABLE test_if_lazy(cond boolean, raw string) USING parquet

statement
INSERT INTO test_if_lazy VALUES (true, '7'), (false, 'bad'), (NULL, 'bad')

-- Only selected rows may reach the fallible cast.
query expect_native(if)
SELECT IF(cond, cast(raw AS INT), 0) FROM test_if_lazy

query expect_error(CAST_INVALID_INPUT)
SELECT IF(cond, 0, cast(raw AS INT)) FROM test_if_lazy

statement
CREATE TABLE test_if_numeric(cond boolean, f float, l bigint) USING parquet

statement
INSERT INTO test_if_numeric VALUES (true, cast('NaN' AS FLOAT), -9223372036854775808L), (false, -0.0F, 9223372036854775807L), (NULL, cast('-Infinity' AS FLOAT), NULL), (true, NULL, 1L), (false, 7.0F, -1L)

query expect_native(if)
SELECT IF(cond, f, 0F), IF(cond, 0F, f), IF(cond, l, 0L), IF(cond, 0L, l) FROM test_if_numeric
