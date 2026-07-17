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

-- Convergent from_unixtime() behavior across all three timeParserPolicy values.
-- Patterns here produce identical output under LEGACY, CORRECTED, and EXCEPTION.
-- ConfigMatrix: spark.sql.legacy.timeParserPolicy=LEGACY,CORRECTED,EXCEPTION
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_from_unix_time_policy(t long) USING parquet

statement
INSERT INTO test_from_unix_time_policy VALUES (0), (1718451045), (-1), (NULL), (2147483647)

query spark_answer_only
SELECT from_unixtime(t) FROM test_from_unix_time_policy

query spark_answer_only
SELECT from_unixtime(t, 'yyyy-MM-dd') FROM test_from_unix_time_policy

query spark_answer_only
SELECT from_unixtime(t, 'yyyy-MM-dd HH:mm:ss') FROM test_from_unix_time_policy

query spark_answer_only
SELECT from_unixtime(t, 'HH:mm:ss') FROM test_from_unix_time_policy

-- literal arguments
query spark_answer_only
SELECT from_unixtime(0)

query spark_answer_only
SELECT from_unixtime(1718451045, 'yyyy-MM-dd')

query spark_answer_only
SELECT from_unixtime(NULL, 'yyyy-MM-dd')
