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

-- timestampdiff runs through the codegen dispatcher so results match Spark exactly.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_timestampdiff(a timestamp, b timestamp) USING parquet

statement
INSERT INTO test_timestampdiff VALUES
  (timestamp'2024-01-01 00:00:00', timestamp'2024-03-15 12:30:00'),
  (timestamp'2024-03-15 12:30:00', timestamp'2024-01-01 00:00:00'),
  (timestamp'2024-01-31 00:00:00', timestamp'2024-02-29 00:00:00'),
  (timestamp'2020-02-29 00:00:00', timestamp'2024-02-29 00:00:00'),
  (NULL, timestamp'2024-01-01 00:00:00'),
  (timestamp'2024-01-01 00:00:00', NULL)

-- whole-unit differences are truncated toward zero, matching Spark
query
SELECT
  timestampdiff(YEAR, a, b),
  timestampdiff(MONTH, a, b),
  timestampdiff(WEEK, a, b),
  timestampdiff(DAY, a, b),
  timestampdiff(HOUR, a, b),
  timestampdiff(MINUTE, a, b),
  timestampdiff(SECOND, a, b)
FROM test_timestampdiff

-- literal arguments (constant folding is disabled by the test suite)
query
SELECT
  timestampdiff(MONTH, timestamp'2024-01-31 00:00:00', timestamp'2024-02-29 00:00:00'),
  timestampdiff(HOUR, timestamp'2024-01-01 00:00:00', timestamp'2024-01-02 06:00:00'),
  timestampdiff(DAY, timestamp'2024-03-15 12:30:00', timestamp'2024-01-01 00:00:00')
