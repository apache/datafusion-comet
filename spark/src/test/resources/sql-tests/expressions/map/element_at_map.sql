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

statement
CREATE TABLE test_element_at_map(m map<string, int>, mi map<int, string>) USING parquet

statement
INSERT INTO test_element_at_map VALUES
  (map('a', 1, 'b', 2, 'c', 3), map(1, 'x', 2, 'y')),
  (map('x', 10), map(99, 'z')),
  (NULL, NULL)

-- key found
query expect_native(element_at)
SELECT element_at(m, 'a'), element_at(m, 'b') FROM test_element_at_map

-- key not found → NULL
query
SELECT element_at(m, 'missing') FROM test_element_at_map

-- null map → NULL
query
SELECT element_at(CAST(NULL AS MAP<STRING, INT>), 'a')

-- null key → NULL
query
SELECT element_at(m, CAST(NULL AS STRING)) FROM test_element_at_map

-- integer key type
query
SELECT element_at(mi, 1), element_at(mi, 2), element_at(mi, 99) FROM test_element_at_map

-- key type coercion
query
SELECT element_at(mi, CAST(1 AS BIGINT)), element_at(mi, CAST(2 AS SMALLINT)) FROM test_element_at_map

-- literal map arguments
query
SELECT element_at(map('a', 1, 'b', 2), 'a'), element_at(map('a', 1, 'b', 2), 'missing'), element_at(map('a', 1, 'b', 2), NULL)

-- Map key types whose Spark equality Comet's native `map_extract` cannot reproduce dispatch to
-- Spark's generated code. These stay on the constructor path here because the SQL harness excludes
-- `ConstantFolding`; `CometMapExpressionSuite` covers the folded-literal form of each.

-- Spark's floating-point equality treats both signs of zero as equal, so a `-0.0` lookup finds
-- the `+0.0` key. Native lookup compares the raw Arrow values.
query expect_dispatch(element_at)
SELECT element_at(map(CAST(0 AS DOUBLE), 7), double('-0.0'))

query expect_dispatch(element_at)
SELECT element_at(map(CAST(0 AS FLOAT), 7), float('-0.0'))

-- The floating-point decline walks every nesting level of the key type, so an array-of-double key
-- dispatches for the same reason.
query expect_dispatch(element_at)
SELECT element_at(map(array(CAST(0 AS DOUBLE)), 7), array(double('-0.0')))

-- A complex key type: `map_extract` casts the lookup key to the map's exact Arrow key type, so a
-- NULL inside the lookup key would abort the cast instead of missing the lookup.
query expect_dispatch(element_at)
SELECT element_at(map(array(1), 7), array(CAST(NULL AS INT)))

query expect_dispatch(element_at)
SELECT element_at(map(named_struct('a', 1), 7), named_struct('a', 1))

-- `BinaryType` keys need no decline: Arrow compares them by content, as Spark's ordering does.
query
SELECT element_at(map(CAST('a' AS BINARY), 1, CAST('b' AS BINARY), 2), CAST('b' AS BINARY))

-- Every key type `MapKeySupport` admits reaches Arrow's `eq`, which is stricter about the exact
-- Arrow type than the `ArrayData` equality it replaced: it distinguishes decimal precision and
-- scale, timestamp time zone, and integer width. Comet's planner casts the lookup key to the type
-- `coerce_types` reports, and the native lookup errors rather than comparing across encodings, so
-- a disagreement between the two shows up as a query failure. Cover the admitted key types that
-- the string/int fixtures above do not.
statement
CREATE TABLE test_element_at_map_keys(
  mb map<boolean, int>,
  mt map<tinyint, int>,
  ms map<smallint, int>,
  ml map<bigint, int>,
  md map<decimal(10,2), int>,
  mdate map<date, int>,
  mts map<timestamp, int>,
  mntz map<timestamp_ntz, int>) USING parquet

statement
INSERT INTO test_element_at_map_keys VALUES (
  map(true, 1, false, 2),
  map(CAST(1 AS TINYINT), 10, CAST(2 AS TINYINT), 20),
  map(CAST(1 AS SMALLINT), 10, CAST(2 AS SMALLINT), 20),
  map(CAST(1 AS BIGINT), 10, CAST(2 AS BIGINT), 20),
  map(CAST(1.50 AS DECIMAL(10,2)), 10, CAST(2.25 AS DECIMAL(10,2)), 20),
  map(DATE '2024-01-01', 10, DATE '2024-06-15', 20),
  map(TIMESTAMP '2024-01-01 00:00:00', 10, TIMESTAMP '2024-06-15 12:30:45', 20),
  map(CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ), 10,
      CAST('2024-06-15 12:30:45' AS TIMESTAMP_NTZ), 20))

query
SELECT element_at(mb, true), element_at(mb, false) FROM test_element_at_map_keys

query
SELECT element_at(mt, CAST(2 AS TINYINT)), element_at(ms, CAST(2 AS SMALLINT)),
       element_at(ml, CAST(2 AS BIGINT)), element_at(ml, CAST(9 AS BIGINT))
FROM test_element_at_map_keys

-- Spark requires a decimal lookup key to have the map's exact precision and scale (a `DECIMAL(5,2)`
-- key against a `MAP<DECIMAL(10,2), INT>` fails analysis with MAP_FUNCTION_DIFF_TYPES), so the
-- planner hands the native lookup a `Decimal128(10, 2)` on both sides and Arrow's `eq` agrees.
query
SELECT element_at(md, CAST(2.25 AS DECIMAL(10,2))), element_at(md, CAST(9.99 AS DECIMAL(10,2)))
FROM test_element_at_map_keys

query
SELECT element_at(mdate, DATE '2024-06-15'), element_at(mdate, DATE '2020-01-01')
FROM test_element_at_map_keys

query
SELECT element_at(mts, TIMESTAMP '2024-06-15 12:30:45'),
       element_at(mts, TIMESTAMP '2020-01-01 00:00:00'),
       element_at(mntz, CAST('2024-06-15 12:30:45' AS TIMESTAMP_NTZ))
FROM test_element_at_map_keys

-- Nested INT-keyed map: the inner `element_at` returns NULL for ids not in the outer map (2, 3),
-- and the outer `element_at` looks it up with a per-row key `id % (id - 2)`. This harness runs with
-- ANSI disabled, so the remainder-by-zero at id = 2 evaluates to NULL rather than throwing, and
-- `element_at(NULL_map, NULL)` returns NULL -- matching Spark's 7, NULL, NULL natively. (Under ANSI,
-- native scalar functions evaluate the key eagerly and throw where Spark short-circuits after the
-- NULL inner map; that is a pre-existing eager-evaluation difference in native `ElementAt`.)
statement
CREATE TABLE test_element_at_nested(id int) USING parquet

statement
INSERT INTO test_element_at_nested VALUES (1), (2), (3)

query
SELECT id, element_at(element_at(map(1, map(0, 7)), id), id % (id - 2)) AS v
FROM test_element_at_nested
