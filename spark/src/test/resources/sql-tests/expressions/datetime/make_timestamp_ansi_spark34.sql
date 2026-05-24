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

-- ANSI mode: make_timestamp throws on out-of-range argument values. Same coverage as
-- make_timestamp_ansi.sql, but the expect_error substrings target the JDK java.time
-- DateTimeException message text directly because Spark 3.4 does not yet wrap these in
-- the DATETIME_FIELD_OUT_OF_BOUNDS error class (that classification arrived in Spark 3.5).
-- The inner JDK message text is stable on Spark 3.4: "Invalid value for MonthOfYear",
-- "Invalid date 'FEBRUARY 30'", "Invalid value for HourOfDay".
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- MaxSparkVersion: 3.4

-- month out of range
query expect_error(MonthOfYear)
SELECT make_timestamp(2024, 13, 1, 0, 0, 0)

-- day out of range
query expect_error(Invalid date)
SELECT make_timestamp(2024, 2, 30, 0, 0, 0)

-- hour out of range
query expect_error(HourOfDay)
SELECT make_timestamp(2024, 6, 15, 25, 0, 0)

-- Sentinel: a valid input must still execute on the Comet codegen path. If the dispatcher
-- silently rejects MakeTimestamp at runtime, the error queries above pass vacuously
-- (Spark and fallback throw identical messages). This non-error query uses
-- `checkSparkAnswerAndOperator` which fails if Comet did not run the expression natively.
query
SELECT make_timestamp(2024, 6, 15, 10, 30, 45.0)
