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

-- MinSparkVersion: 4.1
-- Config: spark.sql.timeType.enabled=true

-- time + day-time interval rewrites to TimeAddInterval ->
-- StaticInvoke(DateTimeUtils.timeAddInterval). Routes through the JVM codegen dispatcher.

statement
CREATE TABLE test_time_add(h int, m int, s decimal(16,6)) USING parquet

statement
INSERT INTO test_time_add VALUES
  (0, 0, 0.000000),
  (10, 15, 30.000000),
  (22, 30, 0.000000),
  (12, 34, 56.789012),
  (NULL, 0, 0.000000)

-- add an hour to a TIME column
query
SELECT make_time(h, m, s) + INTERVAL '1' HOUR FROM test_time_add

-- add fractional-second interval
query
SELECT make_time(h, m, s) + INTERVAL '0.500' SECOND FROM test_time_add

-- literal TIME + literal interval
query
SELECT TIME '00:00:00' + INTERVAL '1' HOUR

query
SELECT TIME '22:30:00' + INTERVAL '30' MINUTE

query
SELECT TIME '10:00:00' + INTERVAL '0.5' SECOND

-- NULL TIME
query
SELECT CAST(NULL AS TIME) + INTERVAL '1' HOUR

-- NULL interval
query
SELECT TIME '10:00:00' + CAST(NULL AS INTERVAL HOUR)
