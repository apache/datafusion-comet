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

-- A cast of a literal that fails under ANSI must not fail the query while Comet plans it. Spark
-- raises the error only when a row reaches the cast, so a branch no row chooses returns rows.
-- ConstantFolding is left enabled: it keeps a failing cast unfolded inside a conditional branch,
-- which is how such a cast reaches Comet in a real query.

-- Config: spark.sql.ansi.enabled=true
-- ConstantFolding: enabled

statement
CREATE TABLE test_cast_literal_ansi(id bigint) USING parquet

statement
INSERT INTO test_cast_literal_ansi VALUES (0), (1), (2)

-- no row reaches the failing cast
query
SELECT id, CASE WHEN id = 5 THEN CAST('bad' AS BIGINT) ELSE id END FROM test_cast_literal_ansi

query
SELECT id, IF(id > 5, CAST('2147483648' AS INT), 0) FROM test_cast_literal_ansi

query
SELECT id, COALESCE(id, CAST('bad' AS INT)) FROM test_cast_literal_ansi

-- Java only allows a D/F suffix after a decimal number, so these special values are invalid
query
SELECT id, CASE WHEN id = 5 THEN CAST('NaNF' AS DOUBLE) ELSE 0D END,
  IF(id > 5, CAST('InfinityD' AS FLOAT), 0F) FROM test_cast_literal_ansi

-- a cast of a literal that does not fail is still accepted
query
SELECT id, CASE WHEN id = 1 THEN CAST('7' AS BIGINT) ELSE id END FROM test_cast_literal_ansi

-- a row reaches the failing cast
query expect_error(CAST_INVALID_INPUT)
SELECT id, CASE WHEN id = 1 THEN CAST('bad' AS BIGINT) ELSE id END FROM test_cast_literal_ansi

query expect_error(CAST_INVALID_INPUT)
SELECT id, CASE WHEN id = 1 THEN CAST('NaNF' AS DOUBLE) ELSE 0D END FROM test_cast_literal_ansi

query expect_error(CAST_INVALID_INPUT)
SELECT id, IF(id > 1, CAST('InfinityD' AS FLOAT), 0F) FROM test_cast_literal_ansi

query expect_error(CAST_OVERFLOW)
SELECT id, IF(id > 1, CAST(2147483648L AS INT), 0) FROM test_cast_literal_ansi

-- The same floating-point parsing applies to a column. Only the exact-case signed NaN is valid.
statement
CREATE TABLE test_cast_float_special(s string) USING parquet

statement
INSERT INTO test_cast_float_special VALUES ('NaNF'), ('NaND'), ('nanf'), ('-nan'), ('+nan'),
  ('-NaN'), ('+NaN'), ('NaN'), ('InfinityD'), ('infF'), ('-InfinityF'), ('Infinity'), ('-inf'),
  ('1.5d'), ('-.5F'), ('1e5d'), ('d'), ('1.0dd'), (NULL)

query
SELECT s, try_cast(s AS DOUBLE), try_cast(s AS FLOAT) FROM test_cast_float_special
