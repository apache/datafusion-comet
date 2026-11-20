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

-- https://github.com/apache/datafusion-comet/issues/7480
-- Config: spark.sql.ansi.enabled=true
-- ConstantFolding: enabled

statement
CREATE TABLE logical_ansi_division(x INT, y INT, a ARRAY<BIGINT>, d DECIMAL(10,2)) USING parquet

-- Keep the zero divisor in the same Parquet row group as the rows that pass the filter.
statement
INSERT INTO logical_ansi_division SELECT /*+ COALESCE(1) */ x, y, a, x AS d FROM VALUES
  (1, 1, array(1L)), (2, 2, NULL), (0, NULL, NULL), (3, 3, NULL) AS t(x, y, a)

-- Spark stops after the first match, before the zero divisor in the same batch.
query
SELECT x FROM logical_ansi_division WHERE y IS NOT NULL AND abs(10 DIV x) > 0 LIMIT 1

-- The projection evaluates its shared decimal division on exactly the rows the limit reads.
query
SELECT d <> 0 AND (1.00 / d) + (1.00 / d) > 0 FROM logical_ansi_division LIMIT 2

query expect_error_class(DIVIDE_BY_ZERO)
SELECT d <> 0 AND (1.00 / d) + (1.00 / d) > 0 FROM logical_ansi_division LIMIT 3

-- Start the second batch with the zero divisor.
statement
SET spark.comet.batchSize=2

-- The comparison uses only x, so Spark's whole-stage filter checks y after dividing.
query expect_error_class(DIVIDE_BY_ZERO)
SELECT x FROM logical_ansi_division WHERE y IS NOT NULL AND abs(10 DIV x) > 0

query expect_error_class(DIVIDE_BY_ZERO)
SELECT x FROM logical_ansi_division WHERE y IS NOT NULL AND 10 / x > 0

query expect_error(BY_ZERO)
SELECT x FROM logical_ansi_division WHERE y IS NOT NULL AND 10 % x > 0

-- The comparison uses y, so Spark checks y before dividing.
query
SELECT x FROM logical_ansi_division WHERE y IS NOT NULL AND abs(10 DIV x) > y

-- The first conjunct returns NULL for x = 0, so the filter discards that row before 10 DIV x.
query
SELECT x FROM logical_ansi_division WHERE try_divide(1, x) > 0 AND abs(10 DIV x) > 0

-- Spark skips the right operand of + when try_divide returns NULL. Until Comet can do
-- that too (#7246), it needs the earlier check on y to keep the zero divisor out of +.
query
SELECT x FROM logical_ansi_division WHERE y IS NOT NULL AND try_divide(1, x) + (10 DIV x) > 0

-- Spark's generated projection evaluates the shared expression before the Boolean guard.
query expect_error_class(DIVIDE_BY_ZERO)
SELECT x <> 0 AND abs(10 DIV x) + abs(10 DIV x) > 1 AS b FROM logical_ansi_division

query expect_error_class(DIVIDE_BY_ZERO)
SELECT x = 0 OR 10 / x > 1, x = 0 OR 10 / x < 8 FROM logical_ansi_division

-- For y = 2, the shared outer division divides by zero before either null array check.
query expect_error_class(DIVIDE_BY_ZERO)
SELECT y <> 0 AND 1 DIV y > 0,
       array_contains(a, 10 DIV (1 DIV y)), array_position(a, 10 DIV (1 DIV y))
FROM logical_ansi_division

-- For y = 1, the shared sum yields 1.0. The lambda rounds away 1.0 before cancellation.
query
SELECT x > 0 AND ((1e20D / y + (-1e20D)) + 1D) > 0, (1e20D / y + (-1e20D)) + 1D,
       transform(a, v -> (1e20D / y + 1D) + (-1e20D))
FROM logical_ansi_division

-- Only 10 DIV y is shared. The guard still protects 10 DIV x.
query
SELECT y, x <> 0 AND (10 DIV x) + (10 DIV y) + (10 DIV y) > 10, x
FROM logical_ansi_division

-- The whole IF is shared, so its own condition still guards the division.
query
SELECT x <> 0 AND IF(y IS NULL, 0, 10 DIV x) + IF(y IS NULL, 0, 10 DIV x) > 1
FROM logical_ansi_division

statement
SET spark.sql.subexpressionElimination.enabled=false

query
SELECT x <> 0 AND abs(10 DIV x) + abs(10 DIV x) > 1 AS b FROM logical_ansi_division

statement
SET spark.sql.subexpressionElimination.enabled=true

-- Standalone generated predicates follow the expression tree. Generated projections
-- still evaluate shared subexpressions up front.
statement
SET spark.sql.codegen.hugeMethodLimit=1

query
SELECT x FROM logical_ansi_division WHERE y IS NOT NULL AND abs(10 DIV x) > 0

statement
SET spark.sql.codegen.hugeMethodLimit=65535

statement
SET spark.sql.codegen.wholeStage=false

query
SELECT x FROM logical_ansi_division WHERE y IS NOT NULL AND abs(10 DIV x) > 0

query expect_error_class(DIVIDE_BY_ZERO)
SELECT x <> 0 AND abs(10 DIV x) + abs(10 DIV x) > 1 AS b FROM logical_ansi_division

statement
SET spark.sql.codegen.factoryMode=NO_CODEGEN

query
SELECT x <> 0 AND abs(10 DIV x) + abs(10 DIV x) > 1 AS b FROM logical_ansi_division
