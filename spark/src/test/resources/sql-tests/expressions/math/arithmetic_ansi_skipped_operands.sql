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

-- Spark evaluates the second operand of an arithmetic operator only for the rows where the first
-- one is not NULL, so an operand that fails under ANSI mode must not fail on the rows Spark skips
-- (https://github.com/apache/datafusion-comet/issues/7246). Every table is one Parquet file, so its
-- rows arrive in one batch.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.adaptive.enabled=false

-- `txt` is malformed only on the rows where `n` is NULL
statement
CREATE TABLE skipped(id int, n int, txt string) USING parquet

statement
INSERT INTO skipped SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, CAST(NULL AS INT), 'bad'), (2, 3, '6'), (3, CAST(NULL AS INT), 'x'), (4, 2, '7')
  AS t(id, n, txt)

-- `n` is NULL on every row
statement
CREATE TABLE all_null(id int, n int, txt string) USING parquet

statement
INSERT INTO all_null SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, CAST(NULL AS INT), 'bad'), (2, CAST(NULL AS INT), '2') AS t(id, n, txt)

-- `n` is not NULL, so Spark evaluates every operand
statement
CREATE TABLE filled(id int, n int, txt string) USING parquet

statement
INSERT INTO filled SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, 1, 'bad'), (2, 2, '2') AS t(id, n, txt)

-- The divisor `n` is zero on every row
statement
CREATE TABLE zero(id int, n int, txt string) USING parquet

statement
INSERT INTO zero SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, 0, CAST(NULL AS STRING)), (2, 0, CAST(NULL AS STRING)) AS t(id, n, txt)

statement
CREATE TABLE zero_bad(id int, n int, txt string) USING parquet

statement
INSERT INTO zero_bad SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, 0, 'bad') AS t(id, n, txt)

-- Add, subtract and multiply evaluate the right operand only where the left one is not NULL

query
SELECT id, n + CAST(txt AS INT) FROM skipped

query
SELECT id, n - CAST(txt AS INT) FROM skipped

query
SELECT id, n * CAST(txt AS INT) FROM skipped

query
SELECT id, CAST(n AS BIGINT) + CAST(txt AS BIGINT) FROM skipped

query
SELECT id, CAST(n AS TINYINT) + CAST(txt AS TINYINT) FROM skipped

query
SELECT id, CAST(n AS SMALLINT) * CAST(txt AS SMALLINT) FROM skipped

query
SELECT id, CAST(n AS DOUBLE) + CAST(txt AS DOUBLE) FROM skipped

query
SELECT id, CAST(n AS DOUBLE) - CAST(txt AS DOUBLE) FROM skipped

query
SELECT id, CAST(n AS DOUBLE) * CAST(txt AS DOUBLE) FROM skipped

-- TRY arithmetic
query
SELECT id, try_add(n, CAST(txt AS INT)) FROM skipped

query
SELECT id, CAST(n AS DECIMAL(10, 2)) * CAST(txt AS DECIMAL(10, 2)) FROM skipped

-- A sum that needs more than 38 digits
query
SELECT id, CAST(n AS DECIMAL(38, 0)) + CAST(txt AS DECIMAL(38, 0)) FROM skipped

-- Divide, integral divide and remainder evaluate the divisor first, and the dividend only where
-- the divisor is not NULL

query
SELECT id, CAST(txt AS INT) DIV n FROM skipped

query
SELECT id, CAST(txt AS BIGINT) DIV CAST(n AS BIGINT) FROM skipped

query
SELECT id, CAST(txt AS INT) / n FROM skipped

query
SELECT id, CAST(txt AS INT) % n FROM skipped

query
SELECT id, CAST(txt AS SMALLINT) DIV CAST(n AS SMALLINT) FROM skipped

query
SELECT id, CAST(txt AS TINYINT) % CAST(n AS TINYINT) FROM skipped

query
SELECT id, try_divide(CAST(txt AS INT), n) FROM skipped

query
SELECT id, CAST(txt AS DECIMAL(10, 2)) / CAST(n AS DECIMAL(10, 2)) FROM skipped

query
SELECT id, CAST(txt AS DECIMAL(10, 2)) DIV CAST(n AS DECIMAL(10, 2)) FROM skipped

-- A remainder that needs more than 38 digits
query
SELECT id, CAST(txt AS DECIMAL(38, 10)) % CAST(n AS DECIMAL(38, 0)) FROM skipped

-- A zero divisor is not NULL, so the dividend is evaluated, and a NULL dividend gives NULL

query
SELECT id, CAST(txt AS INT) DIV n FROM zero

query
SELECT id, CAST(txt AS INT) % n FROM zero

query
SELECT id, CAST(txt AS INT) / n FROM zero

-- No row evaluates the second operand

query
SELECT id, n + CAST(txt AS INT) FROM all_null

query
SELECT id, CAST(txt AS INT) DIV n FROM all_null

-- An operand that Spark evaluates still fails

query expect_error_class(CAST_INVALID_INPUT)
SELECT id, n + CAST(txt AS INT) FROM filled

query expect_error_class(CAST_INVALID_INPUT)
SELECT id, CAST(txt AS INT) DIV n FROM filled

query expect_error_class(CAST_INVALID_INPUT)
SELECT id, CAST(txt AS DECIMAL(38, 10)) % CAST(n AS DECIMAL(38, 0)) FROM filled

-- Spark evaluates the dividend before it checks the divisor for zero
query expect_error_class(CAST_INVALID_INPUT)
SELECT id, CAST(txt AS INT) DIV n FROM zero_bad

query expect_error_class(CAST_INVALID_INPUT)
SELECT id, try_add(n, CAST(txt AS INT)) FROM filled

-- The left operand of an add comes first, so it fails even where the right one is NULL
query expect_error_class(CAST_INVALID_INPUT)
SELECT id, CAST(txt AS INT) + n FROM skipped

-- The divisor comes first, so it fails even where the dividend is NULL
query expect_error_class(CAST_INVALID_INPUT)
SELECT id, n DIV CAST(txt AS INT) FROM skipped

query expect_error_class(CAST_INVALID_INPUT)
SELECT id, n % CAST(txt AS INT) FROM skipped
