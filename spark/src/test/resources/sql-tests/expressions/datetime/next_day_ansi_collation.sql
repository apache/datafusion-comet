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
