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

-- Spark's generated code for concat evaluates each argument only for the rows where every earlier
-- one is not NULL, so an argument that fails under ANSI mode must not fail on the rows Spark skips
-- (https://github.com/apache/datafusion-comet/issues/7246). Every table is one Parquet file, so its
-- rows arrive in one batch.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.adaptive.enabled=false

statement
CREATE TABLE items(id int, txt string) USING parquet

statement
INSERT INTO items SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, 'bad'), (2, '2') AS t(id, txt)

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

-- `n` is NULL on three rows in four, where `txt` is malformed and `big - 1` overflows
statement
CREATE TABLE mostly_null(id int, n int, txt string, big int) USING parquet

statement
INSERT INTO mostly_null SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, CAST(NULL AS INT), 'bad', -2147483648), (2, CAST(NULL AS INT), 'x', -2147483648),
  (3, CAST(NULL AS INT), 'y', -2147483648), (4, 2, '7', 5)
  AS t(id, n, txt, big)

query
SELECT id, concat(IF(id = 1, CAST(NULL AS STRING), CAST(id AS STRING)), CAST(CAST(txt AS INT) AS STRING))
FROM items

query
SELECT id, concat(CAST(id AS STRING), CAST(n AS STRING), CAST(CAST(txt AS INT) AS STRING))
FROM skipped

-- Most rows skip the second argument

query
SELECT id, concat(CAST(n AS STRING), CAST(big - 1 AS STRING)) FROM mostly_null

query
SELECT id, concat(CAST(n AS STRING), CAST(CAST(txt AS INT) AS STRING)) FROM mostly_null

-- No row evaluates the last argument
query
SELECT id, concat(CAST(n AS STRING), CAST(CAST(txt AS INT) AS STRING)) FROM all_null

-- Arguments that cannot fail, which Comet evaluates for every row
query
SELECT id, concat(CAST(n AS STRING), CAST(id AS STRING), CAST(CAST(id AS DECIMAL(12, 2)) AS STRING),
  CAST(id = 2 AS STRING), CAST(CAST(id AS DOUBLE) AS STRING))
FROM skipped

-- An argument that Spark evaluates still fails

query expect_error_class(CAST_INVALID_INPUT)
SELECT id, concat(CAST(n AS STRING), CAST(CAST(txt AS INT) AS STRING)) FROM filled

-- The first argument is evaluated for every row
query expect_error_class(CAST_INVALID_INPUT)
SELECT id, concat(CAST(CAST(txt AS INT) AS STRING), CAST(n AS STRING)) FROM skipped
