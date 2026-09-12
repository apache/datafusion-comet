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

-- second(TIME) rewrites to SecondsOfTime -> StaticInvoke(DateTimeUtils.getSecondsOfTime).
-- EXTRACT(SECOND FROM TIME) rewrites to SecondsOfTimeWithFraction ->
-- StaticInvoke(DateTimeUtils.getSecondsOfTimeWithFraction) which returns Decimal(8,6).
-- Both StaticInvoke forms route through the JVM codegen dispatcher.

statement
CREATE TABLE test_second_time(h int, m int, s decimal(16,6)) USING parquet

statement
INSERT INTO test_second_time VALUES
  (0, 0, 0.000000),
  (1, 2, 3.500000),
  (12, 30, 45.123456),
  (23, 59, 59.999999),
  (0, 0, NULL)

query
SELECT second(make_time(h, m, s)) FROM test_second_time

query
SELECT second(TIME '00:00:00')

query
SELECT second(TIME '13:45:07')

query
SELECT second(TIME '23:59:59.999999')

query
SELECT second(CAST(NULL AS TIME))

-- EXTRACT(SECOND FROM TIME) preserves fractional part as Decimal(8,6)
query
SELECT EXTRACT(SECOND FROM make_time(h, m, s)) FROM test_second_time

query
SELECT EXTRACT(SECOND FROM TIME '00:00:00')

query
SELECT EXTRACT(SECOND FROM TIME '13:45:07.123456')

query
SELECT EXTRACT(SECOND FROM TIME '23:59:59.999999')
