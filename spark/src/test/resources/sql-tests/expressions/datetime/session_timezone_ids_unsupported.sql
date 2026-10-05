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

-- Native code cannot express an offset with seconds, so expressions that need the session
-- timezone do not run natively.
-- https://github.com/apache/datafusion-comet/issues/6329

-- Config: spark.sql.session.timeZone=+05:45:30

statement
CREATE TABLE test_session_tz_seconds(ts timestamp) USING parquet

statement
INSERT INTO test_session_tz_seconds VALUES (TIMESTAMP'2024-01-15 18:30:45Z'), (NULL)

-- casts go through the codegen dispatcher
query
SELECT CAST(ts AS STRING), CAST(ts AS DATE) FROM test_session_tz_seconds

-- hour has no dispatcher path, so it falls back to Spark
query expect_fallback(cannot be represented in native code)
SELECT hour(ts) FROM test_session_tz_seconds
