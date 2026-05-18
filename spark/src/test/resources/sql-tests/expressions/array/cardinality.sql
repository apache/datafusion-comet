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

-- cardinality() is an alias for size() with legacySizeOfNull=false:
-- it always returns NULL for NULL input (never -1), and supports
-- both array and map inputs.
-- inputTypes: TypeCollection(ArrayType, MapType) -> test both

statement
CREATE TABLE test_cardinality(
  arr        array<int>,
  nested_arr array<array<int>>,
  struct_arr array<struct<a: int>>,
  m          map<string, int>
) USING parquet

statement
INSERT INTO test_cardinality VALUES
  (array(1, 2, 3), array(array(1, 2), array(3)), array(named_struct('a', 1), named_struct('a', 2)), map('a', 1, 'b', 2)),
  (array(10),      array(array(10)),              array(named_struct('a', 1)),                       map('x', 99)),
  (array(),        array(),                       array(),                                           map()),
  (NULL,           NULL,                          NULL,                                              NULL)

-- column reference: array input
query
SELECT cardinality(arr) FROM test_cardinality

-- column reference: map input
query
SELECT cardinality(m) FROM test_cardinality

-- both in same query
query
SELECT cardinality(arr), cardinality(m) FROM test_cardinality

-- cardinality returns NULL for NULL input (not -1 like size() in legacy mode)
query
SELECT cardinality(arr), cardinality(m) FROM test_cardinality WHERE arr IS NULL

-- nested array input
query
SELECT cardinality(nested_arr) FROM test_cardinality

-- array-of-structs input
query
SELECT cardinality(struct_arr) FROM test_cardinality

-- literal array and map arguments (spark_answer_only: CreateArray/CreateMap not yet natively supported)
query spark_answer_only
SELECT cardinality(array(1, 2, 3)), cardinality(array()), cardinality(cast(NULL as array<int>))

query spark_answer_only
SELECT cardinality(map('a', 1, 'b', 2)), cardinality(map()), cardinality(cast(NULL as map<string,int>))