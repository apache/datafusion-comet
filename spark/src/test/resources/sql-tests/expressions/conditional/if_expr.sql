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
CREATE TABLE test_if(cond boolean, a int, b int) USING parquet

statement
INSERT INTO test_if VALUES (true, 1, 2), (false, 1, 2), (NULL, 1, 2), (true, NULL, 2), (false, 1, NULL)

query
SELECT IF(cond, a, b) FROM test_if

query
SELECT IF(a > 0, 'positive', 'non-positive') FROM test_if

-- literal arguments
query
SELECT IF(true, 1, 2), IF(false, 1, 2), IF(NULL, 1, 2)

-- A NullType result whose rows come from both branches runs natively. Spark folds
-- `IF(c, NULL, NULL)` itself, so the branch has to be a non-foldable NullType expression.
query expect_native(if)
SELECT IF(cond, aggregate(array(a), NULL, (acc, x) -> NULL), NULL) FROM test_if

-- Both branches have the same Spark type (Spark's IF ignores nullability when it compares them),
-- but native `named_struct` declares a field built from a literal non-nullable and one built from
-- a column nullable. When no row takes the THEN branch, the result is the ELSE as it is unless the
-- planner coerces the branches to one type, which it does for IF and CASE WHEN alike.
statement
CREATE TABLE test_if_branch_types(id bigint) USING parquet

-- One file, so the rows share a batch.
statement
INSERT INTO test_if_branch_types SELECT id FROM range(0, 4, 1, 1)

query expect_native(if)
SELECT IF(id = -1, named_struct('i', 1L, 'n', NULL), named_struct('i', id, 'n', NULL)) FROM test_if_branch_types

query expect_native(if)
SELECT IF(id = -1, named_struct('i', 1L), named_struct('i', id)) FROM test_if_branch_types

query expect_native(casewhen)
SELECT CASE WHEN id = -1 THEN named_struct('i', 1L, 'n', NULL) ELSE named_struct('i', id, 'n', NULL) END FROM test_if_branch_types

-- Spark names a merged struct's fields after the first branch, a native CASE after its ELSE
-- branch, and with case-insensitive analysis the two can differ. Each CASE WHEN or coalesce
-- branch is cast to the expression's own type (native IF already takes the THEN branch's
-- names), so a consumer that compares types exactly (here `array(...)`) sees Spark's names on
-- both of its arguments.
statement
CREATE TABLE test_if_branch_names(id bigint, s struct<a:bigint>) USING parquet

statement
INSERT INTO test_if_branch_names SELECT id, named_struct('a', id + 10) FROM range(0, 4, 1, 1)

query expect_native(if)
SELECT array(IF(id >= 0, s, named_struct('A', id)), s) FROM test_if_branch_names

query expect_native(casewhen)
SELECT array(CASE WHEN id >= 0 THEN s ELSE named_struct('A', id) END, s) FROM test_if_branch_names

query expect_native(coalesce)
SELECT array(coalesce(s, named_struct('A', id)), s) FROM test_if_branch_names

-- `array_append` (`array_insert` on Spark 4.0+) widens its array and item to one element type
-- only when their Spark types differ; here they match, so a CASE item named after its ELSE
-- branch reached the kernel's exact type check.
query expect_native(casewhen)
SELECT array_append(array(s), CASE WHEN id >= 0 THEN named_struct('a', id) ELSE named_struct('A', id) END) FROM test_if_branch_names
