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

-- Spark preserves the first THEN's struct field names when branches differ only in case.
-- Native to_json exposes Arrow field names that the row comparison would otherwise ignore.
-- https://github.com/apache/datafusion-comet/issues/6482
-- Config: spark.sql.caseSensitive=false
-- Config: spark.comet.expression.StructsToJson.allowIncompatible=true

statement
CREATE TABLE test_case_field_names(p boolean, q boolean, i int, d double) USING parquet

statement
INSERT INTO test_case_field_names VALUES
  (true, false, 1, 1.5), (false, true, 2, 2.5),
  (NULL, false, 3, 3.5), (false, NULL, NULL, NULL)

-- The result keeps the THEN name for both selected branches, in either name order.
query expect_native(casewhen)
SELECT
  to_json(CASE WHEN p THEN named_struct('x', i) ELSE named_struct('X', i) END),
  to_json(CASE WHEN p THEN named_struct('X', i) ELSE named_struct('x', i) END)
FROM test_case_field_names

-- Preserve the first THEN's names at both levels of a nested struct, in either branch order.
query expect_native(casewhen)
SELECT
  to_json(CASE WHEN p THEN named_struct('a', named_struct('x', i))
    ELSE named_struct('A', named_struct('X', i)) END),
  to_json(CASE WHEN p THEN named_struct('A', named_struct('X', i))
    ELSE named_struct('a', named_struct('x', i)) END)
FROM test_case_field_names

-- Later THEN branches and an omitted ELSE must also preserve the first THEN's name.
query expect_native(casewhen)
SELECT
  to_json(CASE WHEN p THEN named_struct('x', i)
    WHEN q THEN named_struct('X', i + 10) ELSE named_struct('X', i + 20) END),
  to_json(CASE WHEN p THEN named_struct('x', i)
    WHEN q THEN named_struct('X', i + 10) END)
FROM test_case_field_names

-- A typed NULL in the first THEN still determines the result's struct field name.
query expect_native(casewhen)
SELECT to_json(CASE WHEN p THEN CAST(NULL AS STRUCT<x: INT>)
  WHEN q THEN named_struct('x', i) ELSE named_struct('X', i) END)
FROM test_case_field_names

-- Fields align by position. Matching these names would incorrectly combine INT with DOUBLE.
query expect_native(casewhen)
SELECT to_json(CASE WHEN p THEN named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))
  ELSE named_struct('X', 0, 'x', d) END)
FROM test_case_field_names

-- Case-distinct names repeat across positions in the opposite order. Spark aligns fields by
-- position, so x stays an INT. Pairing fields by name would widen it to DOUBLE, and to_json would
-- emit 7.0. Each INSERT writes its own files, and a batch never spans files, so the first has
-- every row take THEN, the second every row take ELSE, and the third mixes them.
-- https://github.com/apache/datafusion-comet/issues/6532
statement
CREATE TABLE test_case_positional(q boolean, i int, d double) USING parquet

statement
INSERT INTO test_case_positional VALUES (true, 7, 9.5), (true, NULL, NULL)

statement
INSERT INTO test_case_positional VALUES (false, 7, 9.5), (false, NULL, NULL)

statement
INSERT INTO test_case_positional VALUES (true, 7, 9.5), (false, 8, 10.5), (NULL, NULL, NULL), (false, 3, NULL)

-- Both branch orders
query expect_native(casewhen)
SELECT
  to_json(CASE WHEN q THEN named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))
    ELSE named_struct('X', 0, 'x', d) END),
  to_json(CASE WHEN q THEN named_struct('X', 0, 'x', d)
    ELSE named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)) END)
FROM test_case_positional

-- Several branches, none of which has the others' names
query expect_native(casewhen)
SELECT
  to_json(CASE WHEN q THEN named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))
    WHEN i > 7 THEN named_struct('X', 1, 'x', d)
    ELSE named_struct('x', 2, 'X', d) END),
  to_json(CASE WHEN q THEN named_struct('X', 1, 'x', d)
    WHEN i > 7 THEN named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))
    ELSE named_struct('X', 2, 'x', d) END)
FROM test_case_positional

-- Native to_json supports structs, so extract the struct after reconciling the array or map.
query expect_native(casewhen)
SELECT
  to_json(CASE WHEN q THEN array(named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)))
    ELSE array(named_struct('X', 0, 'x', d)) END[0]),
  to_json(CASE WHEN q THEN array(named_struct('X', 0, 'x', d))
    ELSE array(named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))) END[0])
FROM test_case_positional

query expect_native(casewhen)
SELECT
  to_json(CASE WHEN q THEN map('k', named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)))
    ELSE map('k', named_struct('X', 0, 'x', d)) END['k']),
  to_json(CASE WHEN q THEN map('k', named_struct('X', 0, 'x', d))
    ELSE map('k', named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))) END['k'])
FROM test_case_positional

-- Nested structs, and an array inside a map value
query expect_native(casewhen)
SELECT
  to_json(CASE WHEN q THEN named_struct('s', named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)))
    ELSE named_struct('s', named_struct('X', 0, 'x', d)) END),
  to_json(CASE WHEN q THEN map('k', array(named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))))
    ELSE map('k', array(named_struct('X', 0, 'x', d))) END['k'][0]),
  to_json(CASE WHEN q THEN map('k', array(named_struct('X', 0, 'x', d)))
    ELSE map('k', array(named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)))) END['k'][0])
FROM test_case_positional
