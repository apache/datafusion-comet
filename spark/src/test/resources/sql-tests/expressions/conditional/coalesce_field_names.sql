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

-- COALESCE lowers to CASE WHEN with its last argument as ELSE, but must retain the first
-- argument's struct field names. Native to_json exposes names that row comparison ignores.
-- https://github.com/apache/datafusion-comet/issues/6482
-- Config: spark.sql.caseSensitive=false
-- Config: spark.comet.expression.StructsToJson.allowIncompatible=true

statement
CREATE TABLE test_coalesce_field_names(p boolean, q boolean, i int) USING parquet

statement
INSERT INTO test_coalesce_field_names VALUES
  (true, false, 1), (false, true, 2), (NULL, false, 3), (false, NULL, NULL)

query expect_native(coalesce)
SELECT to_json(coalesce(
  CASE WHEN p THEN named_struct('x', i) END,
  CASE WHEN q THEN named_struct('X', i + 100) END,
  named_struct('X', 7)))
FROM test_coalesce_field_names

-- Case-distinct names repeat across positions in the opposite order. Spark aligns fields by
-- position, so x stays an INT. Pairing fields by name would widen it to DOUBLE, and to_json would
-- emit 7.0. Each INSERT writes its own files, and a batch never spans files, so the first has
-- every row take the first argument, the second every row take the last, and the third mixes them.
-- https://github.com/apache/datafusion-comet/issues/6532
statement
CREATE TABLE test_coalesce_positional(p boolean, i int, d double) USING parquet

statement
INSERT INTO test_coalesce_positional VALUES (true, 7, 9.5), (true, NULL, NULL)

statement
INSERT INTO test_coalesce_positional VALUES (false, 7, 9.5), (false, NULL, NULL)

statement
INSERT INTO test_coalesce_positional VALUES (true, 7, 9.5), (false, 8, 10.5), (NULL, NULL, NULL), (false, 3, NULL)

-- Both argument orders
query expect_native(coalesce)
SELECT
  to_json(coalesce(
    CASE WHEN p THEN named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)) END,
    named_struct('X', 0, 'x', d))),
  to_json(coalesce(
    CASE WHEN p THEN named_struct('X', 0, 'x', d) END,
    named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))))
FROM test_coalesce_positional

-- Several arguments, none of which has the others' names
query expect_native(coalesce)
SELECT
  to_json(coalesce(
    CASE WHEN p THEN named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)) END,
    CASE WHEN i > 7 THEN named_struct('X', 1, 'x', d) END,
    named_struct('x', 2, 'X', d))),
  to_json(coalesce(
    CASE WHEN p THEN named_struct('X', 1, 'x', d) END,
    CASE WHEN i > 7 THEN named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)) END,
    named_struct('X', 2, 'x', d)))
FROM test_coalesce_positional

-- Native to_json supports structs, so extract the struct after reconciling the array or map.
query expect_native(coalesce)
SELECT
  to_json(coalesce(
    CASE WHEN p THEN array(named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))) END,
    array(named_struct('X', 0, 'x', d)))[0]),
  to_json(coalesce(
    CASE WHEN p THEN array(named_struct('X', 0, 'x', d)) END,
    array(named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))))[0])
FROM test_coalesce_positional

query expect_native(coalesce)
SELECT
  to_json(coalesce(
    CASE WHEN p THEN map('k', named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))) END,
    map('k', named_struct('X', 0, 'x', d)))['k']),
  to_json(coalesce(
    CASE WHEN p THEN map('k', named_struct('X', 0, 'x', d)) END,
    map('k', named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))))['k'])
FROM test_coalesce_positional

-- Nested structs, and an array inside a map value
query expect_native(coalesce)
SELECT
  to_json(coalesce(
    CASE WHEN p THEN named_struct('s', named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))) END,
    named_struct('s', named_struct('X', 0, 'x', d)))),
  to_json(coalesce(
    CASE WHEN p THEN map('k', array(named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)))) END,
    map('k', array(named_struct('X', 0, 'x', d))))['k'][0]),
  to_json(coalesce(
    CASE WHEN p THEN map('k', array(named_struct('X', 0, 'x', d))) END,
    map('k', array(named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)))))['k'][0])
FROM test_coalesce_positional
