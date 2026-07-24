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

-- timestampadd runs through the codegen dispatcher so results match Spark exactly.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_timestampadd(ts timestamp, q int) USING parquet

statement
INSERT INTO test_timestampadd VALUES
  (timestamp'2024-01-15 10:30:45', 3),
  (timestamp'2024-01-31 23:00:00', 1),
  (timestamp'2024-02-29 12:00:00', 12),
  (timestamp'2024-12-31 23:59:59', 2),
  (timestamp'1970-01-01 00:00:00', -5),
  (NULL, 1),
  (timestamp'2024-06-15 00:00:00', NULL)

-- column quantity across a range of units, including month-end and leap-day rollover
query
SELECT timestampadd(HOUR, q, ts) FROM test_timestampadd

query
SELECT timestampadd(MONTH, q, ts) FROM test_timestampadd

query
SELECT
  timestampadd(YEAR, 1, ts),
  timestampadd(QUARTER, 1, ts),
  timestampadd(WEEK, 2, ts),
  timestampadd(DAY, -10, ts),
  timestampadd(MINUTE, 90, ts),
  timestampadd(SECOND, 30, ts),
  timestampadd(MICROSECOND, 500, ts)
FROM test_timestampadd

-- literal arguments (constant folding is disabled by the test suite)
query
SELECT
  timestampadd(HOUR, 3, timestamp'2024-01-01 10:00:00'),
  timestampadd(MONTH, 1, timestamp'2024-01-31 00:00:00'),
  timestampadd(YEAR, 1, timestamp'2024-02-29 00:00:00')
