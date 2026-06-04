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

-- ANSI mode: Spark's NextDay throws on a malformed dayOfWeek (SparkIllegalArgumentException /
-- ILLEGAL_DAY_OF_WEEK on 3.5+, IllegalArgumentException on 3.4) when spark.sql.ansi.enabled=true.
-- Comet's native next_day now throws the same "Illegal input for day of week" message under ANSI
-- instead of returning NULL.
-- Config: spark.sql.ansi.enabled=true

-- Sentinel: a recognised day name must still execute natively under ANSI. This guards against the
-- expect_error queries passing vacuously if next_day were to fall back to Spark.
query
SELECT next_day(date('2024-01-01'), 'Monday')

-- Unrecognised day name.
query expect_error(Illegal input for day of week)
SELECT next_day(date('2024-01-01'), 'NOT_A_DAY')

-- Empty string is not a valid day name.
query expect_error(Illegal input for day of week)
SELECT next_day(date('2024-01-01'), '')
