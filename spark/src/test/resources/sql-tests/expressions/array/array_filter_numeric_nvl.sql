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

-- `filter(arr, x -> nvl(x, '') != '')` on a numeric, boolean, date or timestamp array is the
-- customer's "drop the empty elements" rule. Spark plans it as
-- `NOT (coalesce(CAST(x AS STRING), '') = '')`, which no cast result can make false except a
-- NULL element, so Comet runs it as `array_compact` instead of casting every element to a string.
-- Each query must return what Spark returns, including for NULL arrays, empty arrays and
-- NULL elements. The string and non-matching shapes at the end must keep their lambda semantics.
-- There is no boolean query: Spark refuses `coalesce(boolean_value, '')`, so `nvl(x, '')` cannot be
-- written on a boolean array.

-- Config: spark.sql.ansi.enabled=false

statement
CREATE TABLE test_filter_numeric_nvl(
  id int,
  strs array<string>,
  ints array<int>,
  longs array<bigint>,
  dbls array<double>,
  flts array<float>,
  decs array<decimal(10, 2)>,
  dates array<date>,
  tss array<timestamp>,
  bools array<boolean>,
  tinyints array<tinyint>,
  smallints array<smallint>
) USING parquet

statement
INSERT INTO test_filter_numeric_nvl VALUES
  (1, array('a', NULL, '', 'b'), array(1, NULL, 0, 5, -3), array(1L, NULL, 0L, 9000000000L),
   array(1.5, NULL, CAST('NaN' AS DOUBLE), -0.0, 0.0, CAST('Infinity' AS DOUBLE)),
   array(CAST(1.5 AS FLOAT), NULL, CAST('NaN' AS FLOAT), CAST(2.5 AS FLOAT)),
   array(CAST(1.25 AS DECIMAL(10, 2)), NULL, CAST(-3.50 AS DECIMAL(10, 2)), CAST(0 AS DECIMAL(10, 2))),
   array(DATE '2020-01-01', NULL, DATE '2024-02-29'),
   array(TIMESTAMP '2020-01-01 00:00:00', NULL, TIMESTAMP '2024-02-29 12:34:56'),
   array(true, NULL, false),
   array(CAST(1 AS TINYINT), NULL, CAST(-128 AS TINYINT)),
   array(CAST(1 AS SMALLINT), NULL, CAST(32767 AS SMALLINT))),
  (2, array(), array(), array(), array(), array(), array(), array(), array(), array(), array(), array()),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (4, array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL),
   array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL)),
  (5, array('x', 'y'), array(7, 8, 9), array(7L, 8L), array(7.0, 8.0), array(CAST(7 AS FLOAT)),
   array(CAST(7 AS DECIMAL(10, 2))), array(DATE '2000-01-01'), array(TIMESTAMP '2000-01-01 00:00:00'),
   array(true), array(CAST(7 AS TINYINT)), array(CAST(7 AS SMALLINT)))

query expect_native(filter)
SELECT id, filter(ints, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(ints, x -> nvl(x, '') <> '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(ints, x -> '' != nvl(x, '')) FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(ints, x -> coalesce(x, '') != '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(longs, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(dbls, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(flts, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(decs, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(dates, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(tss, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(tinyints, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT id, filter(smallints, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

-- The result is a usable array: nested in other functions, and in the shape of the rule.
query expect_native(filter)
SELECT id, size(filter(ints, x -> nvl(x, '') != '')), array_max(filter(longs, x -> nvl(x, '') != ''))
FROM test_filter_numeric_nvl

query expect_native(filter)
SELECT sum(size(filter(ints, x -> nvl(x, '') != ''))) FROM test_filter_numeric_nvl

-- A string element has no cast: empty strings are dropped as well as NULLs.
query expect_native(filter)
SELECT id, filter(strs, x -> nvl(x, '') != '') FROM test_filter_numeric_nvl

-- Not the same predicate, so these keep their lambda semantics: another default value, the equal
-- operator, and a body that tests more than the element.
query
SELECT id, filter(ints, x -> nvl(x, '0') != '0') FROM test_filter_numeric_nvl

query
SELECT id, filter(ints, x -> nvl(x, '') = '') FROM test_filter_numeric_nvl

query
SELECT id, filter(ints, x -> nvl(x, '') != '' AND x > 0) FROM test_filter_numeric_nvl
