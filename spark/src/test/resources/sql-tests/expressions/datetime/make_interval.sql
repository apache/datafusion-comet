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

-- Config: spark.comet.shuffle.mode=native

statement
CREATE TABLE test_make_interval(
  years int,
  months int,
  weeks int,
  days int,
  hours int,
  mins int,
  secs decimal(18, 6)) USING parquet

statement
INSERT INTO test_make_interval VALUES
  -- Adapted from Spark's MakeInterval expression tests:
  -- https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/IntervalExpressionsSuite.scala#L195-L231
  (0, 0, 0, 0, 0, 0, 0.000000),
  (-123, 0, 0, 0, 0, 0, 0.000000),
  (0, 0, 123, 0, 0, 0, 0.000000),
  (0, 0, 0, 0, 0, 0, -0.123000),
  (9999, 11, 0, 31, 23, 59, 59.999999),
  (10000, 0, 0, 0, 0, 0, -0.000001),
  (-9999, -11, 0, -31, -23, -59, -59.999999),
  (-10000, 0, 0, 0, 0, 0, 0.000001),
  (0, 0, 0, 0, 2147483647, 2147483647, 2149633277.790647),
  (100, 11, 1, 1, 12, 30, 1.001001),
  (1, 2, 3, 4, 5, 6, 7.123456),
  (0, 1, 0, 1, 0, 0, 100.000001),
  (-1, -2, -1, -1, -1, -1, -1.500000),
  (NULL, 1, 2, 3, 4, 5, 6.000000),
  (2, NULL, 2, 3, 4, 5, 6.000000),
  (3, 1, 2, 3, 4, 5, NULL),
  (-178956970, -8, -306783378, -2, -2147483648, -2147483648, -999999999999.999999),
  (-2147483648, 0, 0, 0, 0, 0, 0.000000)

query
SELECT make_interval(years, months, weeks, days, hours, mins, secs)
FROM test_make_interval
ORDER BY years

query
-- Adapted from Spark's SQL and DataFrame API default-argument tests:
-- https://github.com/apache/spark/blob/v4.2.0/sql/core/src/test/resources/sql-tests/inputs/interval.sql#L81-L90
-- https://github.com/apache/spark/blob/v4.2.0/sql/core/src/test/scala/org/apache/spark/sql/DateFunctionsSuite.scala#L1284-L1323
SELECT make_interval(),
       make_interval(1),
       make_interval(1, 2),
       make_interval(1, 2, 3),
       make_interval(1, 2, 3, 4),
       make_interval(1, 2, 3, 4, 5),
       make_interval(1, 2, 3, 4, 5, 6),
       make_interval(1, 2, 3, 4, 5, 6, 7.008009)

query
SELECT make_interval(years),
       make_interval(years, months),
       make_interval(years, months, weeks),
       make_interval(years, months, weeks, days),
       make_interval(years, months, weeks, days, hours),
       make_interval(years, months, weeks, days, hours, mins),
       make_interval(years, months, weeks, days, hours, mins, secs)
FROM test_make_interval
WHERE years = 100

query
-- Build ARRAY<INTERVAL> from native make_interval below a native shuffle, then consume its
-- interval child with native GetArrayItem above the shuffle. This pins the CalendarInterval
-- metadata across both native expression and shuffle boundaries.
SELECT intervals, intervals[0]
FROM (
  SELECT years, array(make_interval(years, months, weeks, days, hours, mins, secs)) AS intervals
  FROM test_make_interval
  DISTRIBUTE BY years
)

-- Spark negates, adds and subtracts calendar intervals with IntervalUtils. CometUnaryMinus,
-- CometAdd and CometSubtract only admit numeric types, so these projections stay on Spark rather
-- than handing the interval struct to a numeric kernel.
query expect_fallback(Unsupported datatype CalendarIntervalType)
SELECT -make_interval(years, months, weeks, days, hours, mins, secs),
       make_interval(years) + make_interval(0, months),
       make_interval(years, months, weeks, days) - make_interval(0, 0, 0, 0, hours, mins, secs)
FROM test_make_interval

-- The same arithmetic over intervals that native make_interval built below a native shuffle, so
-- Spark reads them back from the interval struct.
query expect_fallback(Unsupported datatype CalendarIntervalType)
SELECT -i, i + make_interval(years), i - make_interval(0, months)
FROM (
  SELECT years, months, make_interval(years, months, weeks, days, hours, mins, secs) AS i
  FROM test_make_interval
  DISTRIBUTE BY years
)

query
SELECT make_interval(0, 1, 0, 1, 0, 0, 100.000001)

query
SELECT make_interval(2147483647)

query
SELECT make_interval(1, 2, 3, 4, 0, 0, 123456789012.123456)

query
SELECT make_interval(0, 0, 0, 0, 0, 0, 999999999.999999)

query
SELECT make_interval(0, 0, 0, 0, 0, 0, 999999999.000001)

query
SELECT make_interval(0, 0, 0, 0, 2562048)

query
SELECT make_interval(0, 0, 0, 0, 0, 0, 1234567890123456789)

statement
CREATE TABLE test_make_interval_hash(d decimal(38, 10), y int) USING parquet

statement
INSERT INTO test_make_interval_hash VALUES
  (1.5, 1),
  (-123456789.0123456789, NULL),
  (NULL, 2),
  (9999999999999999999999999999.9999999999, 3)

-- hash has no native path above decimal precision 18. make_interval then runs as a whole through
-- the JVM codegen dispatcher, so the projection stays in Comet.
query expect_dispatch(make_interval)
SELECT make_interval(0, 0, 0, hash(d), y) FROM test_make_interval_hash

-- abs converts natively before hash fails. It then runs in the dispatcher kernel, so the explain
-- output reports it as dispatched only.
query expect_dispatch(make_interval, hash, abs)
SELECT make_interval(0, 0, 0, hash(d), abs(y)) FROM test_make_interval_hash

-- A dispatched CAST reads the interval struct that native make_interval wrote below a shuffle.
query expect_dispatch(cast)
SELECT years, CAST(i AS STRING)
FROM (
  SELECT years, make_interval(years, months, weeks, days, hours, mins, secs) AS i
  FROM test_make_interval
  DISTRIBUTE BY years
)

statement
CREATE TABLE test_make_interval_merge(id int, y int) USING parquet

statement
INSERT INTO test_make_interval_merge VALUES (0, 1), (1, NULL), (2, -2), (3, 3), (4, NULL)

-- Native make_interval merged with a NULL interval literal. Spark drops ELSE NULL and NULL
-- coalesce arguments, so the literal reaches Comet as a THEN or IF branch.
query
SELECT id,
  CASE WHEN id % 2 = 0 THEN make_interval(y) ELSE NULL END,
  CASE WHEN id % 2 = 0 THEN NULL ELSE make_interval(y, 1) END,
  if(id > 1, make_interval(0, y), NULL),
  if(id > 1, NULL, make_interval(0, 0, y))
FROM test_make_interval_merge

query
SELECT id,
  coalesce(make_interval(y), CAST(NULL AS INTERVAL)),
  coalesce(make_interval(y), make_interval(0, id)),
  coalesce(CASE WHEN id % 2 = 0 THEN NULL ELSE make_interval(y) END, make_interval(0, 0, 0, id))
FROM test_make_interval_merge

statement
CREATE TABLE test_make_interval_groups(g int, y int) USING parquet

statement
INSERT INTO test_make_interval_groups VALUES
  (1, 1), (1, 1), (2, NULL), (2, NULL), (3, NULL), (3, 3), (NULL, 4)

-- first and last keep the interval layout through the partial and final aggregates. Each group
-- here holds one value, so the answer does not depend on row order.
query
SELECT g, first(make_interval(y)), last(make_interval(y))
FROM test_make_interval_groups
WHERE g <> 3 OR g IS NULL
GROUP BY g

-- Group 3 mixes NULL and 3 years, so only the ignoreNulls form has one answer there.
query
SELECT g, first(make_interval(y), true), last(make_interval(y), true)
FROM test_make_interval_groups
GROUP BY g
