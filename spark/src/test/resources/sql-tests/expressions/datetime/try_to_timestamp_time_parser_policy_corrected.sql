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

-- try_to_timestamp() under CORRECTED timeParserPolicy.
-- Strict java.time parsing returns null for inputs that legacy would accept.
-- MinSparkVersion: 4.0
-- Config: spark.sql.legacy.timeParserPolicy=CORRECTED
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_try_to_ts_strict(s string) USING parquet

statement
INSERT INTO test_try_to_ts_strict VALUES
  ('2024-1-1'),
  ('2024-13-01'),
  ('2024-02-30'),
  ('2024-01-01garbage'),
  ('2024')

query spark_answer_only
SELECT s, try_to_timestamp(s, 'yyyy-MM-dd') FROM test_try_to_ts_strict ORDER BY s
