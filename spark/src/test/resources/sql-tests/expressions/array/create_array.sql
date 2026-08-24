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

-- Array of maps built from primitive int values. Under Spark constant folding this collapses
-- each `map(...)` to a MapType Literal; CometLiteral expands it back to a CreateMap of int
-- literals so the outer `array(...)` runs natively via `make_array`.
query
SELECT array(map(1, 10), map(2, 20))

-- Array of maps whose values are arrays. This is the exact fallback shape reported in
-- https://github.com/apache/datafusion-comet — `MapType(IntegerType, ArrayType(IntegerType, false), true)`.
query
SELECT array(map(1, array(1, 2, 3)), map(2, array(4, 5, 6)))

-- Array of maps with a NULL value inside (map value expansion must handle NULLs).
query
SELECT array(map('x', CAST(NULL AS INT), 'y', 2), map('z', 3))

-- Array containing a folded NULL map literal alongside a non-null map.
query
SELECT array(CAST(NULL AS MAP<INT,INT>), map(1, 2))

-- Deeply nested folded complex literal: array of struct of map of array of struct.
query
SELECT array(named_struct('m', map(1, array(named_struct('id', 1, 's', 'x')))))

-- Array of maps with string keys.
query
SELECT array(map('x', 1, 'y', 2), map('z', 3))

-- Single-element array of map.
query
SELECT array(map(1, 2))

-- Array of arrays of maps (recursive expansion of ArrayType(ArrayType(MapType)) literal).
query
SELECT array(array(map(1, 'a')), array(map(2, 'b')))

-- Array of structs. Under constant folding each named_struct(...) folds to a StructType
-- Literal; expansion rebuilds it as CreateNamedStruct of primitive literals.
query
SELECT array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y'))

-- Column-based array of maps. With constant folding disabled by the SQL-file harness this
-- flows through CometCreateMap's codegen dispatch, not through Literal expansion, but it
-- must remain natively supported. Filter out NULL keys because Spark forbids them.
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

-- Empty complex ArrayType literal takes the pre-expansion makeListLiteral path (empty
-- ListLiteral) so its element type survives even without any children.
query
SELECT CAST(array() AS ARRAY<ARRAY<INT>>)
