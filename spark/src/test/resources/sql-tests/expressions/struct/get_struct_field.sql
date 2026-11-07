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

-- PushFoldableIntoBranches is excluded for the null struct query at the end, which it would
-- rewrite so that native no longer reads a field of the struct. The other queries keep a native
-- GetStructField either way.
-- ExcludeRules: org.apache.spark.sql.catalyst.optimizer.PushFoldableIntoBranches

statement
CREATE TABLE test_struct(s struct<name: string, age: int, score: double>) USING parquet

statement
INSERT INTO test_struct VALUES (named_struct('name', 'Alice', 'age', 30, 'score', 95.5)), (named_struct('name', 'Bob', 'age', 25, 'score', 88.0)), (NULL)

query
SELECT s.name, s.age, s.score FROM test_struct

query
SELECT s.name, s.age + 1, s.score * 2 FROM test_struct

-- A NullType field reads as NULL, also through a null struct, whose nulls are pushed into the
-- all-null field.
statement
CREATE TABLE test_struct_null_field(a int) USING parquet

statement
INSERT INTO test_struct_null_field VALUES (1), (-1), (NULL)

query
SELECT s.x, s.n FROM (
  SELECT IF(a > 0, named_struct('x', a, 'n', null), null) AS s FROM test_struct_null_field)

-- A field of a struct that native evaluates as a scalar (a cast of named_struct over a scalar
-- subquery) is one value for every row. The table is one file, so at the default batch size its
-- four rows share a batch. expect_native checks that the field access itself reaches native, so
-- that an optimizer rewrite cannot turn these into tests of something else.
statement
CREATE TABLE test_struct_scalar(a int) USING parquet

statement
INSERT INTO test_struct_scalar SELECT /*+ REPARTITION(1) */ * FROM VALUES (1), (-1), (NULL), (4) AS v(a)

query expect_native(getstructfield)
SELECT a, CAST(named_struct('x', (SELECT max(a) FROM test_struct_scalar)) AS STRUCT<x: BIGINT>).x, CAST(named_struct('x', (SELECT max(a) FROM test_struct_scalar WHERE a > 100)) AS STRUCT<x: BIGINT>).x FROM test_struct_scalar

-- The same holds for an array, a struct and a map field.
query expect_native(getstructfield)
SELECT a, CAST(named_struct('l', array((SELECT max(a) FROM test_struct_scalar))) AS STRUCT<l: ARRAY<BIGINT>>).l FROM test_struct_scalar

query expect_native(getstructfield)
SELECT a, CAST(named_struct('s', named_struct('y', (SELECT max(a) FROM test_struct_scalar))) AS STRUCT<s: STRUCT<y: BIGINT>>).s FROM test_struct_scalar

-- The map comes from map_from_arrays, which runs natively; map() over a scalar subquery would run
-- in Spark.
query expect_native(getstructfield, map_from_arrays)
SELECT a, CAST(named_struct('m', map_from_arrays(array('k'), array((SELECT max(a) FROM test_struct_scalar)))) AS STRUCT<m: MAP<STRING, BIGINT>>).m FROM test_struct_scalar

-- A field of a null struct that native evaluates as a scalar (an IF over a scalar subquery whose
-- other branch is NULL) is NULL for every row, next to the same IF selecting the struct.
query expect_native(getstructfield)
SELECT a, IF((SELECT max(a) FROM test_struct_scalar) > 100, named_struct('x', (SELECT max(a) FROM test_struct_scalar)), NULL).x, IF((SELECT max(a) FROM test_struct_scalar) > 0, named_struct('x', (SELECT max(a) FROM test_struct_scalar)), NULL).x FROM test_struct_scalar
