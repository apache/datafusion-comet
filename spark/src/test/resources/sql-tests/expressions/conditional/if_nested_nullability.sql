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

-- Spark adds no cast to IF when its branches differ only in whether a nested struct field, map
-- value or array element can be NULL, or in the case of a struct field name, so the native planner
-- has to cast a branch to the common type, as it does for CASE WHEN. Without the cast, a batch in
-- which every row took the ELSE branch returned the ELSE array unchanged, and the query failed with
-- "column types must match schema types". A batch that mixed the two failed when the THEN branch
-- was the one that could not be NULL.
-- https://github.com/apache/datafusion-comet/issues/6334
--
-- The harness disables ConstantFolding, so these cover the constructor path. The folded map
-- literal path is covered in CometMapExpressionSuite.
--
-- Native to_json writes the field names of the Arrow struct it is given, so it shows the names the
-- native IF gave its result. By default, to_json runs through the codegen dispatcher, which
-- evaluates its whole argument, the IF included, in the JVM.
-- Config: spark.comet.expression.StructsToJson.allowIncompatible=true

statement
CREATE TABLE test_if_nested(q boolean, i int, m map<string, int>, s struct<x: int>, ms map<string, struct<x: int>>) USING parquet

-- Each INSERT writes its own files, and a batch never spans files. Every row of this one takes the
-- THEN branch of IF(q, ...) and the ELSE branch of IF(m IS NULL, ...).
statement
INSERT INTO test_if_nested VALUES (true, 1, map('a', 1), named_struct('x', 1), map('a', named_struct('x', 1))), (true, NULL, map('b', CAST(NULL AS INT)), named_struct('x', CAST(NULL AS INT)), map('b', CAST(NULL AS STRUCT<x: INT>)))

-- Every row of this one takes the ELSE branch of IF(q, ...) and the THEN branch of IF(m IS NULL, ...)
statement
INSERT INTO test_if_nested VALUES (false, 2, NULL, NULL, NULL), (NULL, NULL, NULL, NULL, NULL)

-- Pairs of rows that take different branches, so these batches mix the two
statement
INSERT INTO test_if_nested VALUES (true, 3, map('c', 3), named_struct('x', 3), map('c', named_struct('x', 3))), (false, 4, NULL, NULL, NULL), (true, NULL, map('d', CAST(NULL AS INT)), named_struct('x', CAST(NULL AS INT)), map('d', named_struct('x', CAST(NULL AS INT)))), (NULL, 6, NULL, NULL, NULL), (true, 7, map('e', 7), named_struct('x', 7), map('e', CAST(NULL AS STRUCT<x: INT>))), (false, NULL, NULL, NULL, NULL), (true, 9, map('f', 9), named_struct('x', 9), map('f', named_struct('x', 9))), (false, 10, NULL, NULL, NULL), (true, 11, map('g', 11), named_struct('x', 11), map('g', named_struct('x', 11))), (NULL, 12, NULL, NULL, NULL)

-- struct constructors: x can be NULL in one branch only
query
SELECT IF(q, named_struct('x', i), named_struct('x', 0)) FROM test_if_nested

query
SELECT IF(q, named_struct('x', 0), named_struct('x', i)) FROM test_if_nested

-- Different fields are nullable in each branch, so both branches need a cast to the common type.
query expect_native(if)
SELECT IF(q, named_struct('x', i, 'y', 0), named_struct('x', 0, 'y', i)) FROM test_if_nested

-- Reconcile nested struct fields through an array, in both branch orders.
query expect_native(if)
SELECT IF(q, array(named_struct('x', i)), array(named_struct('x', 0))), IF(q, array(named_struct('x', 0)), array(named_struct('x', i))) FROM test_if_nested

-- Recurse through a struct into map values and array elements, in both branch orders.
query expect_native(if)
SELECT IF(q, named_struct('m', map('k', i), 'a', array(i)), named_struct('m', map('k', 0), 'a', array(0))), IF(q, named_struct('m', map('k', 0), 'a', array(0)), named_struct('m', map('k', i), 'a', array(i))) FROM test_if_nested

-- map constructors: the value can be NULL in one branch only
query
SELECT IF(q, map('k', i), map('k', 0)) FROM test_if_nested

query
SELECT IF(q, map('k', 0), map('k', i)) FROM test_if_nested

-- a map column, whose value can be NULL, and a map constructor
query
SELECT IF(q, m, map('z', 0)) FROM test_if_nested

query
SELECT IF(m IS NULL, map('z', 0), m) FROM test_if_nested

-- a struct column and a struct constructor
query
SELECT IF(q, s, named_struct('x', 0)) FROM test_if_nested

-- struct field names that differ only in case, which Spark treats as one type. It adds no cast,
-- and the result's field has the THEN branch's name. Without the cast, a batch that mixed the two
-- failed as well, because DataFusion's struct cast matches fields by name.
query
SELECT IF(q, named_struct('x', i), named_struct('X', i)) FROM test_if_nested

-- The row comparison ignores struct field names, so check them with to_json, in both orders. Spark
-- 4 reports to_json as `invoke`, so this names only the IF, which isn't reported as native if
-- to_json runs through the dispatcher.
query expect_native(if)
SELECT to_json(IF(q, named_struct('x', i), named_struct('X', i))), to_json(IF(q, named_struct('X', i), named_struct('x', i))) FROM test_if_nested

-- a struct inside a map value
query
SELECT IF(q, ms, map('z', named_struct('x', 0))) FROM test_if_nested

-- an array inside a map value
query
SELECT IF(q, map('k', array(i)), map('k', array(0))) FROM test_if_nested

-- arrays already agree, because CreateArray casts its elements to one type
query
SELECT IF(q, array(i), array(0)) FROM test_if_nested

-- CASE WHEN already casts its branches to a common type
query
SELECT CASE WHEN q THEN named_struct('x', i) ELSE named_struct('x', 0) END FROM test_if_nested

-- Case-distinct names repeat across positions in the opposite order. Spark aligns fields by
-- position; name-based type union would pair INT with DOUBLE and make to_json emit 7.0.
statement
CREATE TABLE test_if_positional(q boolean, i int, d double) USING parquet

statement
INSERT INTO test_if_positional VALUES (true, 7, 9.5), (true, NULL, NULL)

-- Pin the all-THEN wrong result before adding rows that exercise ELSE and mixed batches.
query expect_native(if)
SELECT to_json(IF(q, named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)), named_struct('X', 0, 'x', d))) FROM test_if_positional

statement
INSERT INTO test_if_positional VALUES (false, 7, 9.5), (false, NULL, NULL)

statement
INSERT INTO test_if_positional VALUES (true, 7, 9.5), (false, 8, 10.5), (NULL, NULL, NULL)

query expect_native(if)
SELECT to_json(IF(q, named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)), named_struct('X', 0, 'x', d))), to_json(IF(q, named_struct('X', 0, 'x', d), named_struct('x', i, 'X', CAST(5.5 AS DOUBLE)))) FROM test_if_positional

-- Native to_json supports structs, so extract the struct after reconciling the array or map.
query expect_native(if)
SELECT to_json(IF(q, array(named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))), array(named_struct('X', 0, 'x', d)))[0]) FROM test_if_positional

query expect_native(if)
SELECT to_json(IF(q, map('k', named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))), map('k', named_struct('X', 0, 'x', d)))['k']) FROM test_if_positional

query expect_native(if)
SELECT to_json(IF(q, named_struct('s', named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))), named_struct('s', named_struct('X', 0, 'x', d)))) FROM test_if_positional
