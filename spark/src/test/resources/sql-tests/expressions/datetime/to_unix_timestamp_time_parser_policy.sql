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

-- Convergent to_unix_timestamp(string, format) behavior across all three policies.
-- ConfigMatrix: spark.sql.legacy.timeParserPolicy=LEGACY,CORRECTED,EXCEPTION
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_to_unix_ts_policy(s string) USING parquet

statement
INSERT INTO test_to_unix_ts_policy VALUES ('2024-06-15 10:30:45'), ('1970-01-01 00:00:00'), (NULL), ('')

query spark_answer_only
SELECT to_unix_timestamp(s, 'yyyy-MM-dd HH:mm:ss') FROM test_to_unix_ts_policy

query spark_answer_only
SELECT to_unix_timestamp(s) FROM test_to_unix_ts_policy

statement
CREATE TABLE test_to_unix_ts_date_policy(s string) USING parquet

statement
INSERT INTO test_to_unix_ts_date_policy VALUES ('2024-06-15'), ('1970-01-01'), (NULL)

query spark_answer_only
SELECT to_unix_timestamp(s, 'yyyy-MM-dd') FROM test_to_unix_ts_date_policy

query spark_answer_only
SELECT to_unix_timestamp('2024-06-15', 'yyyy-MM-dd')

query spark_answer_only
SELECT to_unix_timestamp(NULL, 'yyyy-MM-dd')
