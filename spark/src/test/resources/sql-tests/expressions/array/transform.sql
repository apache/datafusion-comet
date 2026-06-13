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

-- Higher-order function transform. No native (rust) implementation; runs through Comet's codegen
-- dispatcher, so the projection stays native and matches Spark.

statement
CREATE TABLE test_transform(
  a array<int>,
  s array<string>,
  factor int,
  nested array<array<int>>,
  structs array<struct<x: int, y: string>>,
  maps array<map<string, int>>) USING parquet

statement
INSERT INTO test_transform VALUES
  (array(1, 2, 3), array('a', 'b'), 10,
    array(array(1, 2), array(3)), array(struct(1, 'a'), struct(2, 'b')), array(map('k', 1))),
  (array(-5, 5), array('x'), 100,
    array(array()), array(struct(3, 'c')), array(map('p', 9), map('q', 8))),
  (array(), array(), 0, array(), array(), array()),
  (NULL, NULL, NULL, NULL, NULL, NULL)

-- basic
query
SELECT transform(a, x -> x + 1) FROM test_transform

-- lambda with element index
query
SELECT transform(a, (x, i) -> x + i) FROM test_transform

-- column capture: lambda references another column from the row
query
SELECT transform(a, x -> x + factor) FROM test_transform

-- string elements
query
SELECT transform(s, x -> concat(x, '!')) FROM test_transform

-- nested array<array<int>>
query
SELECT transform(nested, x -> x) FROM test_transform

-- nested array<array<int>>, inner transform
query
SELECT transform(nested, x -> transform(x, y -> y * 2)) FROM test_transform

-- nested array<struct>, identity passes a complex element through (exercises element copy)
query
SELECT transform(structs, e -> e) FROM test_transform

-- nested array<struct>, project a field into a new struct
query
SELECT transform(structs, e -> struct(e.x + 1 AS x)) FROM test_transform

-- nested array<map>, identity passes a map element through
query
SELECT transform(maps, e -> e) FROM test_transform

-- compare a transform result (containsNull=false) against a nullable-element column: exercises
-- nested-comparison nullability reconciliation
query
SELECT a FROM test_transform WHERE transform(a, x -> array(x)) = nested

-- all literals (constant folding is disabled by the test harness)
query
SELECT transform(array(1, 2, 3), x -> x * x)
