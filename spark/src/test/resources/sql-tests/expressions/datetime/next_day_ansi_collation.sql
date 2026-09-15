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
-- Config: spark.comet.batchSize=1024

-- ANSI mode with a collated dayOfWeek. A collated next_day runs through the JVM codegen
-- dispatcher (see next_day_collation.sql for why), which evaluates Spark's own generated code, so
-- the ANSI error must surface from the Comet pipeline rather than from a Spark fallback.

-- Sentinel: a recognised collated day name must still execute inside the Comet pipeline under
-- ANSI, through the dispatcher rather than natively. Without it the expect_error queries below
-- would pass vacuously if next_day fell back to Spark, which raises the same error.
query
SELECT next_day(date('2024-01-01'), 'Monday' COLLATE UTF8_LCASE)

-- Case-insensitive collation does not make an unrecognised name recognised.
query expect_error(Illegal input for day of week)
SELECT next_day(date('2024-01-01'), 'NOT_A_DAY' COLLATE UTF8_LCASE)

query expect_error(Illegal input for day of week)
SELECT next_day(date('2024-01-01'), 'NOT_A_DAY' COLLATE UNICODE_CI)

-- An RTRIM collation does not trim the day name before matching, so a padded value throws under
-- ANSI rather than resolving to MONDAY.
query expect_error(Illegal input for day of week)
SELECT next_day(date('2024-01-01'), 'MON ' COLLATE UTF8_LCASE_RTRIM)

-- One file keeps the valid first row and invalid second row in the same batch.
statement
CREATE TABLE test_next_day_ansi_masks USING parquet AS
SELECT /*+ COALESCE(1) */ expected, d, dow FROM VALUES
 (date('2024-01-08'), date('2024-01-01'), 'Monday'),
 (CAST(NULL AS DATE), date('2024-01-01'), 'NOT_A_DAY') AS t(expected, d, dow)

-- Spark skips next_day when the comparison's left operand is NULL.
query expect_fallback(next_day requires Spark evaluation under EqualTo)
SELECT expected = next_day(d, dow COLLATE UTF8_LCASE) FROM test_next_day_ansi_masks

-- LIMIT consumes only the valid row; the rest of its batch must remain unevaluated.
query expect_fallback(next_day requires Spark evaluation below LIMIT)
SELECT next_day(d, dow COLLATE UTF8_LCASE) FROM test_next_day_ansi_masks LIMIT 1

-- An invalid row that is actually consumed must still throw.
query expect_error(Illegal input for day of week)
SELECT next_day(d, dow COLLATE UTF8_LCASE) FROM test_next_day_ansi_masks
WHERE dow = 'NOT_A_DAY' LIMIT 1
