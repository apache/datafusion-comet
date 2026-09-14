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

-- TIMEDIFF was added to the timestampdiff grammar rule in Spark 4.0, so it parses to the
-- same TimestampDiff node and runs through the codegen dispatcher. See timestampdiff.sql
-- for the unit and DST coverage that applies to every spelling.
-- MinSparkVersion: 4.0
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_timediff(a timestamp, b timestamp) USING parquet

statement
INSERT INTO test_timediff VALUES
  (timestamp'2024-01-01 00:00:00', timestamp'2024-03-15 12:30:00'),
  (timestamp'2024-03-15 12:30:00', timestamp'2024-01-01 00:00:00'),
  (NULL, timestamp'2024-01-01 00:00:00'),
  (timestamp'2024-01-01 00:00:00', NULL)

query
SELECT
  timediff(YEAR, a, b),
  timediff(MONTH, a, b),
  timediff(DAY, a, b),
  timediff(HOUR, a, b),
  timediff(MICROSECOND, a, b)
FROM test_timediff

-- literal arguments (constant folding is disabled by the test suite)
query
SELECT
  timediff(HOUR, timestamp'2024-03-09 12:00:00', timestamp'2024-03-10 12:00:00'),
  timediff(QUARTER, timestamp'2024-01-01 00:00:00', timestamp'2024-08-15 00:00:00')
