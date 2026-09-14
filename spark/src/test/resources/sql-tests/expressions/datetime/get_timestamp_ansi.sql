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

-- ANSI mode: GetTimestamp throws on parse failure. The codegen dispatcher inherits
-- the throw from Spark's own GetTimestamp.doGenCode. The time parser policy is pinned
-- to CORRECTED so the JDK java.time formatter (and the CANNOT_PARSE_TIMESTAMP error class)
-- is exercised regardless of the runtime default.
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.legacy.timeParserPolicy=CORRECTED
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- The CANNOT_PARSE_TIMESTAMP error class was standardized in Spark 3.5.
-- MinSparkVersion: 3.5

query expect_error(CANNOT_PARSE_TIMESTAMP)
SELECT to_timestamp('not a date', 'yyyy-MM-dd')

query expect_error(CANNOT_PARSE_TIMESTAMP)
SELECT to_timestamp('2024-13-99', 'yyyy-MM-dd')

query expect_error(CANNOT_PARSE_TIMESTAMP)
SELECT to_date('not a date', 'yyyy-MM-dd')

-- try_to_timestamp does NOT throw under ANSI mode (failOnError=false)
query
SELECT try_to_timestamp('not a date', 'yyyy-MM-dd')

-- Sentinel: confirms Comet ran the expression natively. If the dispatcher silently rejects
-- GetTimestamp, the error queries above pass vacuously via Spark fallback. This valid
-- query uses checkSparkAnswerAndOperator and fails if Comet did not execute it natively.
query
SELECT to_timestamp('2024-06-15 10:30:45', 'yyyy-MM-dd HH:mm:ss')
