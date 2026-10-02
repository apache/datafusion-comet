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

-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE test_ts_seconds_fallback(b tinyint, s smallint, d decimal(20, 6), l bigint) USING parquet

statement
INSERT INTO test_ts_seconds_fallback VALUES (1, 1, 1640995200.123456, 1640995200), (NULL, NULL, NULL, NULL)

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT timestamp_seconds(b) FROM test_ts_seconds_fallback

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT timestamp_seconds(s) FROM test_ts_seconds_fallback

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT timestamp_seconds(d) FROM test_ts_seconds_fallback

-- The native input types do not require the dispatcher.
query expect_native(timestamp_seconds)
SELECT timestamp_seconds(l) FROM test_ts_seconds_fallback
