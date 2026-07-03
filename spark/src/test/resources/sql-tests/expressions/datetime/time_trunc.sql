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

-- time_trunc(unit, time) rewrites to TimeTrunc -> StaticInvoke(DateTimeUtils.timeTrunc).
-- Routes through the JVM codegen dispatcher.

statement
CREATE TABLE test_time_trunc(h int, m int, s decimal(16,6)) USING parquet

statement
INSERT INTO test_time_trunc VALUES
  (9, 32, 5.359000),
  (23, 59, 59.999999),
  (0, 0, 0.000000),
  (12, 34, 56.123456),
  (NULL, 0, 0.000000)

query
SELECT time_trunc('HOUR', make_time(h, m, s)) FROM test_time_trunc

query
SELECT time_trunc('MINUTE', make_time(h, m, s)) FROM test_time_trunc

query
SELECT time_trunc('SECOND', make_time(h, m, s)) FROM test_time_trunc

query
SELECT time_trunc('MILLISECOND', make_time(h, m, s)) FROM test_time_trunc

query
SELECT time_trunc('MICROSECOND', make_time(h, m, s)) FROM test_time_trunc

-- literal TIME arguments
query
SELECT time_trunc('HOUR', TIME '09:32:05.359')

query
SELECT time_trunc('MILLISECOND', TIME '09:32:05.123456')

query
SELECT time_trunc('SECOND', TIME '23:59:59.999999')

-- lowercase unit
query
SELECT time_trunc('hour', TIME '13:45:07.999999')
