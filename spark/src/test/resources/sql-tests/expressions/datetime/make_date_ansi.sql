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

-- ANSI mode: Spark's MakeDate wraps the java.time.DateTimeException raised by LocalDate.of in
-- ansiDateTimeArgumentOutOfRange (4.0, DATETIME_FIELD_OUT_OF_BOUNDS) / ansiDateTimeError (3.4/3.5)
-- when spark.sql.ansi.enabled=true. Comet's native SparkMakeDate now throws the same
-- java.time-style message under ANSI instead of returning NULL. The expect_error patterns below
-- are substrings of that message and match across Spark versions.
-- Config: spark.sql.ansi.enabled=true

-- Sentinel: a valid date must still execute natively under ANSI. This guards against the
-- expect_error queries passing vacuously if make_date were to fall back to Spark.
query
SELECT make_date(2024, 2, 28)

-- February 30 is not a valid date.
query expect_error(Invalid date)
SELECT make_date(2024, 2, 30)

-- Month 13 is out of range.
query expect_error(MonthOfYear)
SELECT make_date(2024, 13, 1)

-- Day 0 is out of range.
query expect_error(DayOfMonth)
SELECT make_date(2024, 6, 0)
