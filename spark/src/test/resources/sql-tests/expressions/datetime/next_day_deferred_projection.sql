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

-- MinSparkVersion: 4.0
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.codegen.wholeStage=true
-- Config: spark.sql.codegen.factoryMode=FALLBACK
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.shuffle.partitions=1
-- ConfigMatrix: spark.sql.adaptive.enabled=false,true

statement
CREATE TABLE test_next_day_deferred USING parquet AS
SELECT /*+ COALESCE(1) */ id, d, dow, CAST(NULL AS DATE) AS null_date FROM VALUES
 (1, date('2024-01-01'), 'Monday'),
 (2, date('2024-01-01'), 'NOT_A_DAY') AS t(id, d, dow)

-- A strict parent consumes next_day on every row; deferral alone must not disable Comet.
query
SELECT date_add(x, 1), r FROM (
 SELECT next_day(d, 'Monday' COLLATE UTF8_LCASE) AS x, rand(123L) AS r
 FROM test_next_day_deferred) p

-- rand keeps the lower Project in place. Its BIGINT seed is required for Comet admission.
-- Spark evaluates the filter before evaluating the deferred next_day output.
query expect_fallback(deferred projection)
SELECT x FROM (
 SELECT next_day(d, dow COLLATE UTF8_LCASE) AS x, rand(123L) AS r
 FROM test_next_day_deferred) p
WHERE r < 0.0

-- Null-safe join keys avoid an inferred is-not-null Filter. Only the matching row's payload
-- is evaluated. Retaining r in the output prevents pruning the nondeterministic expression.
query expect_fallback(deferred projection)
SELECT /*+ BROADCAST(k) */ p.x, p.r FROM (
 SELECT id, next_day(d, dow COLLATE UTF8_LCASE) AS x, rand(123L) AS r
 FROM test_next_day_deferred) p
INNER JOIN (SELECT * FROM VALUES (1) AS keys(id)) k ON p.id <=> k.id

-- A parent Project can also skip a lower Project's output, without any Filter or join.
query expect_fallback(deferred projection)
SELECT IF(r >= 0.0, date('2024-01-01'), x) FROM (
 SELECT next_day(d, dow COLLATE UTF8_LCASE) AS x, rand(123L) AS r
 FROM test_next_day_deferred) p

-- A comparison still skips its right operand when its left column is null.
query expect_fallback(deferred projection)
SELECT null_date < x, r FROM (
 SELECT null_date, next_day(d, dow COLLATE UTF8_LCASE) AS x, rand(123L) AS r
 FROM test_next_day_deferred) p

-- try_add catches errors from the deferred next_day inside Spark's generated try block.
query expect_fallback(deferred projection)
SELECT try_add(x, 1), r FROM (
 SELECT next_day(d, dow COLLATE UTF8_LCASE) AS x, rand(123L) AS r
 FROM test_next_day_deferred) p

-- When the filter consumes the invalid row, its deferred expression must still throw.
query expect_error(Illegal input for day of week)
SELECT x FROM (
 SELECT next_day(d, dow COLLATE UTF8_LCASE) AS x, rand(123L) AS r
 FROM test_next_day_deferred) p
WHERE r >= 0.0

-- Spark keeps next_day inside the grouped aggregate's result expressions and evaluates it
-- only after the filter admits a group. The aggregate arguments themselves do not throw.
query expect_fallback(deferred projection)
SELECT d FROM (
 SELECT id,
  next_day(DATE '2024-01-01', CAST(max(id) AS STRING) COLLATE UTF8_LCASE) AS d,
  rand(123L) AS r
 FROM test_next_day_deferred GROUP BY id) a
WHERE r < 0.0

-- An admitted group still evaluates the invalid aggregate result.
query expect_error(Illegal input for day of week)
SELECT d FROM (
 SELECT id,
  next_day(DATE '2024-01-01', CAST(max(id) AS STRING) COLLATE UTF8_LCASE) AS d,
  rand(123L) AS r
 FROM test_next_day_deferred GROUP BY id) a
WHERE r >= 0.0

-- Without grouping keys, Spark evaluates aggregate results before the filter. The invalid
-- result must therefore throw even though the filter would discard it.
query expect_error(Illegal input for day of week)
SELECT d FROM (
 SELECT next_day(DATE '2024-01-01', CAST(max(id) AS STRING) COLLATE UTF8_LCASE) AS d,
  rand(123L) AS r
 FROM test_next_day_deferred) a
WHERE r < 0.0

-- Ungrouped result expressions remain accelerated when their evaluation is safe.
query
SELECT d FROM (
 SELECT next_day(date_add(DATE '2024-01-01', max(id)), 'Monday' COLLATE UTF8_LCASE) AS d,
  rand(123L) AS r
 FROM test_next_day_deferred) a
WHERE r < 0.0
