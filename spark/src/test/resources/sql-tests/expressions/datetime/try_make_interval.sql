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

-- MinSparkVersion: 4.0
-- ConfigMatrix: spark.sql.ansi.enabled=true,false

statement
CREATE TABLE test_try_make_interval(
  years int,
  months int,
  weeks int,
  days int,
  hours int,
  mins int,
  secs decimal(18, 6)) USING parquet

statement
INSERT INTO test_try_make_interval VALUES
  (1, 2, 3, 4, 5, 6, 7.123456),
  (0, 0, 0, 0, 2562048, 0, 999999999.000001),
  (2147483647, 0, 0, 0, 0, 0, 0.000000)

query
SELECT try_make_interval(years, months, weeks, days, hours, mins, secs)
FROM test_try_make_interval
ORDER BY years

query
-- Adapted from Spark's try_make_interval default-argument API tests:
-- https://github.com/apache/spark/blob/v4.2.0/sql/connect/client/jvm/src/test/scala/org/apache/spark/sql/PlanGenerationTestSuite.scala#L2037-L2076
SELECT try_make_interval(1),
       try_make_interval(1, 2),
       try_make_interval(1, 2, 3),
       try_make_interval(1, 2, 3, 4),
       try_make_interval(1, 2, 3, 4, 5),
       try_make_interval(1, 2, 3, 4, 5, 6),
       try_make_interval(1, 2, 3, 4, 5, 6, 7.008009)
