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

-- AND, OR and IN evaluate their later operands only for the rows that the earlier ones have not
-- decided, so an operand that fails under ANSI mode does not fail on a row that Spark skips.
-- Every row is in one batch.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_lazy_logical(v int, s string) USING parquet

statement
INSERT INTO test_lazy_logical SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (0, 'bad'), (1, '5'), (2, '1'), (NULL, '3'), (4, '7') AS t(v, s)

query
SELECT v, v != 0 AND 100 / v > 1 FROM test_lazy_logical WHERE v IS NOT NULL

query
SELECT v, v = 0 OR 100 / v > 1 FROM test_lazy_logical WHERE v IS NOT NULL

query
SELECT v FROM test_lazy_logical WHERE v != 0 AND 100 / v > 1

query
SELECT v FROM test_lazy_logical WHERE v = 0 OR 100 / v > 1

-- A chain, which is rebalanced
query
SELECT v, v != 0 AND v < 10 AND 100 / v > 1 AND CAST(s AS INT) > 0
FROM test_lazy_logical WHERE v IS NOT NULL

-- Nested under another operator
query
SELECT v, NOT (v = 0 OR 100 / v > 1), (v != 0 AND 200 / v > 1) IS NULL
FROM test_lazy_logical WHERE v IS NOT NULL

query
SELECT v, v IN (0, 100 / v) FROM test_lazy_logical WHERE v IS NOT NULL

query
SELECT v, v NOT IN (0, 100 / v) FROM test_lazy_logical WHERE v IS NOT NULL

query
SELECT v, v IN (0, CAST(s AS INT)) FROM test_lazy_logical WHERE v IS NOT NULL

-- A NULL candidate does not stop the later ones from being evaluated
query
SELECT v, v IN (0, CAST(s AS INT), NULL) FROM test_lazy_logical WHERE s != 'bad'

-- A row that no earlier operand decides still fails
query expect_error(DIVIDE_BY_ZERO)
SELECT v != 1 AND 100 / v > 1 FROM test_lazy_logical WHERE v IS NOT NULL

query expect_error(DIVIDE_BY_ZERO)
SELECT v = 1 OR 100 / v > 1 FROM test_lazy_logical WHERE v IS NOT NULL

query expect_error(CAST_INVALID_INPUT)
SELECT v, v IN (1, CAST(s AS INT)) FROM test_lazy_logical WHERE v IS NOT NULL
