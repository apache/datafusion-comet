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

-- ANSI mode: make_timestamp throws on out-of-range argument values. With the codegen
-- dispatcher enabled, Spark's own MakeTimestamp.doGenCode produces the throw site, so
-- Comet's kernel raises the same exception as Spark. The expect_error substrings target
-- the inner JDK java.time.DateTimeException message text (which is wrapped in a
-- SparkDateTimeException whose getMessage() preserves it). The driver-formatted error
-- class string `DATETIME_FIELD_OUT_OF_BOUNDS` is NOT preserved when the exception is
-- thrown from a task on the executor side (only the wrapped `Job aborted ... Lost task
-- ... SparkDateTimeException: <inner message>` form is preserved), so we match the JDK
-- field names which are stable from Spark 3.4 through 4.x.
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

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
-- silently rejects MakeTimestamp at runtime, the operator falls back to Spark and the
-- error queries above pass vacuously (Spark and fallback throw identical messages). This
-- non-error query uses `checkSparkAnswerAndOperator` which fails if Comet did not run the
-- expression natively, so a regression that breaks the dispatch path surfaces here.
query
SELECT make_timestamp(2024, 6, 15, 10, 30, 45.0)
