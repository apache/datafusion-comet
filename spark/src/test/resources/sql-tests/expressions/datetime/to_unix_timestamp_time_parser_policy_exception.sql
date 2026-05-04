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

-- to_unix_timestamp() under EXCEPTION timeParserPolicy (the default).
-- Inputs accepted by legacy but rejected by the new parser raise SparkUpgradeException.
-- Config: spark.sql.legacy.timeParserPolicy=EXCEPTION
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_to_unix_ts_exception(s string) USING parquet

statement
INSERT INTO test_to_unix_ts_exception VALUES ('2024-1-1')

query expect_error(INCONSISTENT_BEHAVIOR_CROSS_VERSION)
SELECT to_unix_timestamp(s, 'yyyy-MM-dd') FROM test_to_unix_ts_exception
