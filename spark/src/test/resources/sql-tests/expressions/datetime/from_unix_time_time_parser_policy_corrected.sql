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

-- from_unixtime() under CORRECTED timeParserPolicy.
-- Patterns recognized only by the legacy formatter raise SparkUpgradeException
-- at formatter creation, even under CORRECTED.
-- Config: spark.sql.legacy.timeParserPolicy=CORRECTED
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_from_unix_time_corrected(t long) USING parquet

statement
INSERT INTO test_from_unix_time_corrected VALUES (1718451045)

query expect_error(INCONSISTENT_BEHAVIOR_CROSS_VERSION)
SELECT from_unixtime(t, 'yyyy-MM-dd aaaa') FROM test_from_unix_time_corrected
