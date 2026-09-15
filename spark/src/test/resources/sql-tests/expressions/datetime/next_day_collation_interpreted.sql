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
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- ConfigMatrix: spark.sql.codegen.factoryMode=NO_CODEGEN,FALLBACK
-- Config: spark.sql.codegen.wholeStage=false

-- The harness excludes ConstantFolding. Restore it so these cases exercise the default
-- optimizer: a failing constant is retained inside IF rather than evaluated during planning.
statement
SET spark.sql.optimizer.excludedRules=

statement
CREATE TABLE test_next_day_collated_interpreted USING parquet AS
SELECT CAST(NULL AS DATE) AS null_d, date('2024-01-01') AS valid_d,
 true AS flag, 'Monday' AS dow, 'NOT_A_DAY' AS bad_dow

-- Safe weekday forms still dispatch. ConstantFolding reduces the collated literal to a Literal,
-- while the column weekday remains nonfoldable.
query
SELECT next_day(valid_d, dow COLLATE UTF8_LCASE),
 next_day(valid_d, 'Monday' COLLATE UTF8_LCASE) FROM test_next_day_collated_interpreted

-- Interpreted NextDay skips its weekday when the date is NULL. Generated evaluation fails while
-- compiling this foldable weekday. FALLBACK must let Spark recover with an interpreted projection.
query expect_fallback(weekday fails during code generation)
SELECT IF(flag,
 next_day(null_d, CAST(1 / 0 AS STRING) COLLATE UTF8_LCASE), date('2024-01-01'))
FROM test_next_day_collated_interpreted

-- AddMonths dispatches its whole subtree, so guarding only the NextDay serde is insufficient.
query expect_fallback(weekday fails during code generation)
SELECT add_months(IF(flag,
 next_day(null_d, CAST(1 / 0 AS STRING) COLLATE UTF8_LCASE), date('2024-01-01')), 0)
FROM test_next_day_collated_interpreted

-- A non-null date consumes the invalid weekday expression and must still raise its error.
query expect_error(DIVIDE_BY_ZERO)
SELECT IF(flag,
 next_day(valid_d, CAST(1 / 0 AS STRING) COLLATE UTF8_LCASE), date('2024-01-01'))
FROM test_next_day_collated_interpreted

-- Without whole-stage fusion, Spark eagerly evaluates the lower Project before the Filter.
-- Returning an empty result here would suppress an error Spark raises in this mode.
query expect_error(Illegal input for day of week)
SELECT x FROM (
 SELECT next_day(valid_d, bad_dow COLLATE UTF8_LCASE) AS x, rand(123L) AS r
 FROM test_next_day_collated_interpreted) p
WHERE r < 0.0
