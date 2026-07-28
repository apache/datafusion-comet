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

-- CAST(string AS date/timestamp) under LEGACY timeParserPolicy.
-- LEGACY / EXCEPTION policies use `SimpleDateFormat` semantics, which the native
-- string-to-datetime kernel does not replicate. `CometCast` reports `Incompatible`
-- and the `CodegenDispatchFallback` mixin routes the cast through the JVM codegen
-- dispatcher (Spark's own `doGenCode` inside the Comet pipeline), so results match
-- Spark exactly without a full Spark fallback.
-- Config: spark.sql.legacy.timeParserPolicy=LEGACY
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_cast_string_dt(s string) USING parquet

statement
INSERT INTO test_cast_string_dt VALUES
  ('2024-01-01'),
  ('2024-1-1'),
  ('2024-13-01'),
  ('2024-02-30'),
  ('2024-01-01garbage'),
  ('2024'),
  (NULL)

-- Cast to DateType under LEGACY timeParserPolicy.
query spark_answer_only
SELECT s, CAST(s AS DATE) FROM test_cast_string_dt ORDER BY s

-- Cast to TimestampType under LEGACY timeParserPolicy.
query spark_answer_only
SELECT s, CAST(s AS TIMESTAMP) FROM test_cast_string_dt ORDER BY s

-- Cast to TimestampNTZType under LEGACY timeParserPolicy.
query spark_answer_only
SELECT s, CAST(s AS TIMESTAMP_NTZ) FROM test_cast_string_dt ORDER BY s
