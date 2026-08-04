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

-- make_interval runs through the codegen dispatcher and produces CalendarIntervalType.
-- The suite disables ANSI mode, so MakeInterval.failOnError is false here and overflow
-- yields NULL. See make_interval_ansi.sql for the throwing path.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_make_interval(y int, mo int, w int, d int, h int, mi int, s decimal(18,6)) USING parquet

statement
INSERT INTO test_make_interval VALUES
  (1, 2, 3, 4, 5, 6, 7.008009),
  (30, 25, 0, -100, 40, 80, 299.889987),
  (0, -1, 0, 1, 0, 0, -1.000000),
  (-1, -2, -3, -4, -5, -6, -7.500000),
  (0, 0, 0, 0, 0, 0, 0.000000),
  (NULL, 2, 3, 4, 5, 6, 7.008009),
  (1, 2, 3, 4, 5, 6, NULL)

-- all seven arguments as columns
query
SELECT make_interval(y, mo, w, d, h, mi, s) FROM test_make_interval

-- the shorter arities filled in by MakeInterval's auxiliary constructors
query
SELECT
  make_interval(y),
  make_interval(y, mo),
  make_interval(y, mo, w),
  make_interval(y, mo, w, d),
  make_interval(y, mo, w, d, h),
  make_interval(y, mo, w, d, h, mi)
FROM test_make_interval

-- mixed literal and column arguments
query
SELECT
  make_interval(1, mo, 3, d, 5, mi, 7.008009),
  make_interval(y, 2, w, 4, h, 6, s)
FROM test_make_interval

-- literal arguments (constant folding is disabled by the test suite)
query
SELECT
  make_interval(1, 2, 3, 4, 5, 6, 7.008009),
  make_interval(0, 0, 0, 0, 0, 0, 0),
  make_interval(-1, -2, -3, -4, -5, -6, -7.5),
  make_interval(0, 13, 0, 0, 25, 61, 61.5)

-- NULL arguments propagate. These have to come from nullable columns rather than NULL
-- literals: MakeInterval is NullIntolerant, so NullPropagation rewrites any call with a
-- literal NULL argument to a null interval literal and the expression never reaches Comet.
-- Such a literal currently fails natively rather than falling back, tracked by #5058.
query
SELECT
  make_interval(y, 2, 3, 4, 5, 6, 7.008009),
  make_interval(1, 2, 3, 4, 5, 6, s),
  make_interval(y)
FROM test_make_interval

-- years * 12 exceeds the int range. Outside ANSI mode MakeInterval swallows the
-- ArithmeticException and returns NULL.
query
SELECT
  make_interval(200000000, 0, 0, 0, 0, 0, 0),
  make_interval(y + 200000000, mo, w, d, h, mi, s)
FROM test_make_interval
