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

-- time_diff(unit, start, end) rewrites to TimeDiff ->
-- StaticInvoke(DateTimeUtils.timeDiff). Result is LongType. Routes through the JVM codegen
-- dispatcher.

statement
CREATE TABLE test_time_diff(h1 int, m1 int, s1 decimal(16,6), h2 int, m2 int, s2 decimal(16,6)) USING parquet

statement
INSERT INTO test_time_diff VALUES
  (20, 30, 29.000000, 21, 30, 29.000000),
  (0, 0, 0.000000, 0, 0, 1.000000),
  (20, 30, 29.000000, 12, 0, 0.000000),
  (0, 0, 0.000000, 23, 59, 59.999999),
  (NULL, 0, 0.000000, 0, 0, 0.000000)

-- column arguments across all supported units
query
SELECT time_diff('HOUR', make_time(h1, m1, s1), make_time(h2, m2, s2)) FROM test_time_diff

query
SELECT time_diff('MINUTE', make_time(h1, m1, s1), make_time(h2, m2, s2)) FROM test_time_diff

query
SELECT time_diff('SECOND', make_time(h1, m1, s1), make_time(h2, m2, s2)) FROM test_time_diff

query
SELECT time_diff('MILLISECOND', make_time(h1, m1, s1), make_time(h2, m2, s2)) FROM test_time_diff

query
SELECT time_diff('MICROSECOND', make_time(h1, m1, s1), make_time(h2, m2, s2)) FROM test_time_diff

-- literal TIME arguments
query
SELECT time_diff('HOUR', TIME '20:30:29', TIME '21:30:29')

query
SELECT time_diff('SECOND', TIME '00:00:00', TIME '23:59:59.999999')

-- negative difference (start > end)
query
SELECT time_diff('HOUR', TIME '20:30:29', TIME '12:00:00')

-- lowercase unit
query
SELECT time_diff('second', TIME '00:00:00', TIME '00:00:01')
