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

-- try_make_timestamp rewrites to MakeTimestamp(failOnError = false), which the codegen
-- dispatcher routes through Spark's MakeTimestamp.doGenCode. Invalid components must produce
-- NULL, not garbage timestamp bytes (issue #4554).
--
-- Function added in Spark 4.0.0.
-- MinSparkVersion: 4.0
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_try_make_ts(y int, mo int, d int, h int, mi int, s decimal(8,6)) USING parquet

statement
INSERT INTO test_try_make_ts VALUES
  (2024, 6, 15, 10, 30, 45.123456),
  (2024, 13, 1, 0, 0, 0.0),
  (2024, 2, 30, 0, 0, 0.0),
  (2024, 6, 32, 0, 0, 0.0),
  (2024, 6, 15, 25, 0, 0.0),
  (2024, 6, 15, 0, 60, 0.0),
  (NULL, 6, 15, 10, 30, 45.0)

query
SELECT try_make_timestamp(y, mo, d, h, mi, s) FROM test_try_make_ts

-- Literal invalid components must also return NULL.
query
SELECT try_make_timestamp(2024, 13, 1, 0, 0, 0.0)

query
SELECT try_make_timestamp(2024, 6, 32, 0, 0, 0.0)

query
SELECT try_make_timestamp(2024, 6, 15, 25, 0, 0.0)

-- Sentinel: a valid input must run on the Comet codegen path. If the dispatcher silently
-- rejected MakeTimestamp the operator would fall back to Spark and the NULL-on-invalid queries
-- above pass vacuously. `query` mode uses checkSparkAnswerAndOperator, which fails if Comet
-- did not run the expression natively.
query
SELECT try_make_timestamp(2024, 6, 15, 10, 30, 45.0)
