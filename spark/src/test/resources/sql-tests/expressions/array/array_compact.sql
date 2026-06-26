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

-- Spark's ArrayCompact is RuntimeReplaceable -> rewrites to
-- ArrayFilter(child, x -> IsNotNull(x)). On Spark 4.0+ that's wrapped in
-- KnownNotContainsNull. Both paths route to DataFusion's array_compact.

statement
CREATE TABLE test_array_compact(
  ints array<int>,
  longs array<bigint>,
  shorts array<smallint>,
  bytes array<tinyint>,
  floats array<float>,
  doubles array<double>,
  decs array<decimal(38, 0)>,
  bools array<boolean>,
  strs array<string>,
  bins array<binary>,
  dates array<date>,
  tss array<timestamp>,
  nested array<array<int>>,
  structs array<struct<a:int, b:string>>
) USING parquet

statement
INSERT INTO test_array_compact VALUES
  (
    array(1, NULL, 2147483647, NULL, -2147483648),
    array(CAST(1 AS BIGINT), NULL, 9223372036854775807, NULL, -9223372036854775808),
    array(CAST(1 AS SMALLINT), NULL, CAST(32767 AS SMALLINT), NULL, CAST(-32768 AS SMALLINT)),
    array(CAST(1 AS TINYINT), NULL, CAST(127 AS TINYINT), NULL, CAST(-128 AS TINYINT)),
    array(CAST(1.0 AS FLOAT), NULL, CAST('NaN' AS FLOAT), CAST('Infinity' AS FLOAT), CAST(-0.0 AS FLOAT)),
    array(CAST(1.0 AS DOUBLE), NULL, CAST('NaN' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST(0.0 AS DOUBLE)),
    array(CAST(1 AS DECIMAL(38, 0)), NULL, CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0))),
    array(true, NULL, false, NULL),
    array('a', NULL, 'é', NULL, '日本', ''),
    array(X'01', NULL, X'02FF', NULL, X''),
    array(DATE '1970-01-01', NULL, DATE '9999-12-31'),
    array(TIMESTAMP '1970-01-01 00:00:00', NULL, TIMESTAMP '9999-12-31 23:59:59'),
    array(array(1, NULL, 3), NULL, array(4, 5)),
    array(named_struct('a', 1, 'b', 'x'), NULL, named_struct('a', 2, 'b', 'y'))
  ),
  (array(), array(), array(), array(), array(), array(), array(), array(), array(), array(), array(), array(), array(), array()),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL),
    array(NULL, NULL)
  ),
  (
    array(1, 2, 3),
    array(CAST(1 AS BIGINT), CAST(2 AS BIGINT)),
    array(CAST(1 AS SMALLINT), CAST(2 AS SMALLINT)),
    array(CAST(1 AS TINYINT), CAST(2 AS TINYINT)),
    array(CAST(1.5 AS FLOAT), CAST(2.5 AS FLOAT)),
    array(CAST(1.5 AS DOUBLE), CAST(2.5 AS DOUBLE)),
    array(CAST(1 AS DECIMAL(38, 0)), CAST(2 AS DECIMAL(38, 0))),
    array(true, false),
    array('x', 'y', 'z'),
    array(X'AA', X'BB'),
    array(DATE '2024-02-29'),
    array(TIMESTAMP '2024-02-29 12:34:56'),
    array(array(1, 2), array(3, 4)),
    array(named_struct('a', 5, 'b', 'z'))
  )

query
SELECT array_compact(ints) FROM test_array_compact

query
SELECT array_compact(longs) FROM test_array_compact

query
SELECT array_compact(shorts) FROM test_array_compact

query
SELECT array_compact(bytes) FROM test_array_compact

query
SELECT array_compact(floats) FROM test_array_compact

query
SELECT array_compact(doubles) FROM test_array_compact

query
SELECT array_compact(decs) FROM test_array_compact

query
SELECT array_compact(bools) FROM test_array_compact

query
SELECT array_compact(strs) FROM test_array_compact

query
SELECT array_compact(bins) FROM test_array_compact

query
SELECT array_compact(dates) FROM test_array_compact

query
SELECT array_compact(tss) FROM test_array_compact

-- nested array<array<int>>: outer NULL elements removed, inner NULL elements preserved
query
SELECT array_compact(nested) FROM test_array_compact

-- array<struct<...>>: NULL struct elements removed, inner field NULLs preserved
query
SELECT array_compact(structs) FROM test_array_compact

-- ============================================================
-- Literal arguments (CometSqlFileTestSuite disables constant folding,
-- so literal-only queries also exercise the native eval path)
-- ============================================================

query
SELECT array_compact(array(1, NULL, 2, NULL, 3))

query
SELECT array_compact(array('a', NULL, 'b'))

query
SELECT array_compact(array(NULL, NULL, NULL))

query
SELECT array_compact(array(CAST(NULL AS INT)))

query
SELECT array_compact(array(1.0, NULL, 2.0, NULL, 3.0))

query
SELECT array_compact(array(array(1, NULL, 3), NULL, array(NULL, 2, 3)))
