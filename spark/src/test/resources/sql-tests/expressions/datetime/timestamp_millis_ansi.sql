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

-- ANSI mode: timestamp_millis throws on overflow. The codegen dispatcher inherits the
-- throw from Spark's own MillisToTimestamp.doGenCode.
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

query expect_error(overflow)
SELECT timestamp_millis(9223372036854775807L)

query expect_error(overflow)
SELECT timestamp_millis(-9223372036854775808L)

-- Sentinel: confirms Comet ran the expression natively. If the dispatcher silently rejects
-- MillisToTimestamp, the error queries above pass vacuously via Spark fallback. This valid
-- query uses `checkSparkAnswerAndOperator` and fails if Comet did not execute it natively.
query
SELECT timestamp_millis(1718451045000)
