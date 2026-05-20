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

-- Convergent date_format() behavior across all three timeParserPolicy values.
-- Patterns here produce identical output under LEGACY, CORRECTED, and EXCEPTION.
-- ConfigMatrix: spark.sql.legacy.timeParserPolicy=LEGACY,CORRECTED,EXCEPTION
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_date_format_policy(ts timestamp) USING parquet

statement
INSERT INTO test_date_format_policy VALUES (timestamp('2024-06-15 10:30:45')), (timestamp('1970-01-01 00:00:00')), (NULL)

query spark_answer_only
SELECT date_format(ts, 'yyyy-MM-dd') FROM test_date_format_policy

query spark_answer_only
SELECT date_format(ts, 'yyyy-MM-dd HH:mm:ss') FROM test_date_format_policy

query spark_answer_only
SELECT date_format(ts, 'HH:mm:ss') FROM test_date_format_policy

query spark_answer_only
SELECT date_format(ts, 'yyyyMMdd') FROM test_date_format_policy

query spark_answer_only
SELECT date_format(ts, 'yyyyMM') FROM test_date_format_policy

-- literal arguments
query spark_answer_only
SELECT date_format(timestamp('2024-06-15 10:30:45'), 'yyyy-MM-dd')

query spark_answer_only
SELECT date_format(NULL, 'yyyy-MM-dd')
