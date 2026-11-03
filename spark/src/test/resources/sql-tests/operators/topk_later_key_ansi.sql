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

-- Spark's top-K ordering evaluates a later sort key only for rows whose earlier keys tie, so a
-- later key that fails under ANSI mode does not fail on a row Spark skips. Comet evaluates every
-- key over the whole batch, so a top-K whose later key can fail runs on Spark (#7246).
-- Each table is one batch.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.adaptive.enabled=false

statement
CREATE TABLE topk(a int, b int) USING parquet

statement
INSERT INTO topk SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, 2147483647), (2, 5), (3, 2147483647), (4, 7), (5, 9) AS t(a, b)

-- No two rows tie on a, so Spark never evaluates b + 1
query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk ORDER BY a, b + 1 LIMIT 2

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk ORDER BY a DESC, CAST(CAST(b AS STRING) AS INT) * 2 LIMIT 2

query expect_fallback(can fail on rows that Spark's ordering skips)
SELECT a, b FROM topk ORDER BY a, abs(b) LIMIT 2

-- Later keys that cannot fail stay native
query
SELECT a, b FROM topk ORDER BY a, b LIMIT 2

query
SELECT a, b FROM topk ORDER BY a, b IS NULL, CAST(b AS BIGINT), try_add(b, 1) LIMIT 2

query
SELECT a, b FROM topk ORDER BY a, substr(CAST(b AS STRING), 1, 3), upper(CAST(b AS STRING)),
  length(lower(CAST(b AS STRING))) LIMIT 2

-- The first key is evaluated for every row in Spark too
query
SELECT a, b FROM topk ORDER BY a + 1, b LIMIT 2

statement
CREATE TABLE topk_ties(a int, b int) USING parquet

statement
INSERT INTO topk_ties SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, 2147483647), (1, 5), (2, 7) AS t(a, b)

-- Rows tie on a, so Spark evaluates b + 1 and fails
query expect_error(ARITHMETIC_OVERFLOW)
SELECT a, b FROM topk_ties ORDER BY a, b + 1 LIMIT 2

-- Without ANSI mode the key cannot fail and stays native
statement
SET spark.sql.ansi.enabled=false

query
SELECT a, b FROM topk ORDER BY a, b + 1 LIMIT 2

query
SELECT a, b FROM topk_ties ORDER BY a, b + 1 LIMIT 2

query
SELECT a, b FROM topk_ties ORDER BY a, -b, abs(b) LIMIT 2
