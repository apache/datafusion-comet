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

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_make_interval_ansi(
  id int,
  years int,
  months int,
  weeks int,
  days int,
  hours int,
  mins int,
  secs decimal(18, 6)) USING parquet

statement
INSERT INTO test_make_interval_ansi VALUES
  -- Adapted from Spark's ANSI MakeInterval expression tests:
  -- https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/IntervalExpressionsSuite.scala#L233-L279
  (0, NULL, 0, 0, 0, 0, 0, 0.000000),
  (1, 0, 0, 0, 0, 0, 0, 0.000000),
  (2, -123, 0, 0, 0, 0, 0, 0.000000),
  (3, 0, 0, 123, 0, 0, 0, 0.000000),
  (4, 0, 0, 0, 0, 0, 0, -0.123000),
  (5, 9999, 11, 0, 31, 23, 59, 59.999999),
  (6, 10000, 0, 0, 0, 0, 0, -0.000001),
  (7, -9999, -11, 0, -31, -23, -59, -59.999999),
  (8, -10000, 0, 0, 0, 0, 0, 0.000001),
  (9, 0, 0, 0, 0, 2147483647, 2147483647, 2149633277.790647),
  (10, 2147483647, 0, 0, 0, 0, 0, 0.000000),
  (11, 0, 0, 2147483647, 0, 0, 0, 0.000000)

query
SELECT make_interval(1, 2, 3, 4, 5, 6, 7.123456)

query
SELECT make_interval(years, months, weeks, days, hours, mins, secs)
FROM test_make_interval_ansi
WHERE id BETWEEN 0 AND 9
ORDER BY id

query expect_error(overflow. If necessary set)
SELECT make_interval(years)
FROM test_make_interval_ansi
WHERE id = 10

query expect_error(overflow. If necessary set)
SELECT make_interval(0, 0, weeks)
FROM test_make_interval_ansi
WHERE id = 11

query
SELECT make_interval(0, 0, 0, 0, 2562048)
