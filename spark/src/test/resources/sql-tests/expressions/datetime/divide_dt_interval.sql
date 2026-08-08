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

-- Routes divide_dt_interval through the codegen dispatcher; produces DayTimeIntervalType.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_divide_dt_interval(days int, hours int, minutes int, seconds decimal(18,6), b tinyint, s smallint, i int, l long, f float, d double, dec decimal(10,2)) USING parquet

statement
INSERT INTO test_divide_dt_interval VALUES
  (1, 2, 3, 4.500000, CAST(2 AS TINYINT), CAST(3 AS SMALLINT), 2, CAST(3 AS BIGINT), CAST(1.5 AS FLOAT), CAST(2.5 AS DOUBLE), CAST(2.50 AS DECIMAL(10, 2))),
  (-1, 0, 30, 15.250000, CAST(-2 AS TINYINT), CAST(-3 AS SMALLINT), -2, CAST(-3 AS BIGINT), CAST(-1.5 AS FLOAT), CAST(-2.5 AS DOUBLE), CAST(-2.50 AS DECIMAL(10, 2))),
  (0, 0, 0, 0.000001, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), 2, CAST(2 AS BIGINT), CAST(2.0 AS FLOAT), CAST(2.0 AS DOUBLE), CAST(2.00 AS DECIMAL(10, 2))),
  (2, -6, 0, 0.000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL)

query
SELECT
  make_dt_interval(days, hours, minutes, seconds) / b,
  make_dt_interval(days, hours, minutes, seconds) / s,
  make_dt_interval(days, hours, minutes, seconds) / i,
  make_dt_interval(days, hours, minutes, seconds) / l,
  make_dt_interval(days, hours, minutes, seconds) / f,
  make_dt_interval(days, hours, minutes, seconds) / d,
  make_dt_interval(days, hours, minutes, seconds) / dec
FROM test_divide_dt_interval

-- literal interval input
query
SELECT INTERVAL '1 02:03:04.500000' DAY TO SECOND / i FROM test_divide_dt_interval

-- literal divisors, including half-up rounding to the nearest microsecond.
query
SELECT
  make_dt_interval(1, 2, 3, 4.5) / 2,
  INTERVAL '0.000001' SECOND / 2,
  INTERVAL '0.000001' SECOND / CAST(2.00 AS DECIMAL(10, 2)),
  make_dt_interval(-1, 0, 30, 15.25) / 1.5D

-- null interval input
query
SELECT make_dt_interval(NULL, hours, minutes, seconds) / 2
FROM test_divide_dt_interval

-- Division by zero fails regardless of ANSI mode.
query expect_error(zero)
SELECT make_dt_interval(1) / 0

-- This interval is Long.MinValue microseconds, which cannot be divided by -1.
query expect_error(overflow)
SELECT make_dt_interval(-106751991, -4, 0, -54.775808) / -1
