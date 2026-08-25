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

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_create_array(a int, b int, c int) USING parquet

statement
INSERT INTO test_create_array VALUES (1, 2, 3), (NULL, 2, 3), (NULL, NULL, NULL)

query
SELECT array(a, b, c) FROM test_create_array

query
SELECT array(1, 2, 3, NULL)

-- Boundary integer values including int min/max.
query
SELECT array(2147483647, -2147483648, 0, NULL)

-- Table for column-based coverage of map/struct/nested-array children.
statement
CREATE TABLE test_create_array_complex(k int, v int, s string, arr array<int>) USING parquet

statement
INSERT INTO test_create_array_complex VALUES
  (1, 10, 'a', array(1, 2, 3)),
  (2, 20, NULL, array(4, NULL, 6)),
  (NULL, NULL, 'c', array()),
  (NULL, NULL, NULL, NULL)

-- Array of nested arrays.
query
SELECT array(array(1, 2), array(3, NULL))

query
SELECT array(array(a, b), array(b, c)) FROM test_create_array

-- Array of maps built from primitive int values. The SQL-file harness always excludes
-- ConstantFolding, so every `map(...)` below stays a `CreateMap` and reaches native
-- `make_array` through the constructor path. Folded-literal expansion in `CometLiteral` is
-- covered by `CometArrayExpressionSuite` instead, where folding is left enabled.
query
SELECT array(map(1, 10), map(2, 20))

-- Array of maps whose values are arrays: `MapType(IntegerType, ArrayType(IntegerType, false), true)`.
query
SELECT array(map(1, array(1, 2, 3)), map(2, array(4, 5, 6)))

-- Array of maps with string keys.
query
SELECT array(map('x', 1, 'y', 2), map('z', 3))

-- Array of maps whose children disagree on `valueContainsNull`. Spark's CreateArray coercion
-- compares element types with `sameType`, which ignores nullability, so it inserts no unifying
-- cast. DataFusion 54.1 cannot unify two Map arguments either: the fallback chain in
-- `type_union_resolution_coercion` has no `map_coercion` arm, so `make_array` receives the two
-- differently typed map arrays and `MutableArrayData` panics. `CometCreateArray` therefore keeps
-- `MapType.valueContainsNull` significant when it normalizes container nullability and declines.
query expect_fallback(CreateArray children have mismatched data types)
SELECT array(map('x', CAST(NULL AS INT), 'y', 2), map('z', 3))

-- Both children agree that the value is nullable, so this stays native.
query
SELECT array(map('x', CAST(NULL AS INT)), map('z', CAST(NULL AS INT)))

-- Single-element array of map.
query
SELECT array(map(1, 2))

-- Array of arrays of maps: `ArrayType(ArrayType(MapType(IntegerType, StringType)))`.
query
SELECT array(array(map(1, 'a')), array(map(2, 'b')))

-- Array of structs.
query
SELECT array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y'))

-- Column-based array of maps. Filter out NULL keys because Spark forbids them.
query
SELECT array(map(k, v)) FROM test_create_array_complex WHERE k IS NOT NULL

-- Column-based array of structs.
query
SELECT array(named_struct('k', k, 's', s)) FROM test_create_array_complex

-- Column-based array of nested arrays.
query
SELECT array(arr, array(k, v)) FROM test_create_array_complex

-- Array of maps whose value is a nested array column.
query
SELECT array(map(k, arr)) FROM test_create_array_complex WHERE k IS NOT NULL

-- Empty array cast to a nested array type: the element type has to survive with no children.
query
SELECT CAST(array() AS ARRAY<ARRAY<INT>>)
