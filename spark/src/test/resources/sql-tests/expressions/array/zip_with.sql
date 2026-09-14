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

-- Higher-order function zip_with. Runs through Comet's codegen dispatcher.

statement
CREATE TABLE test_zip_with(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_zip_with VALUES
  (array(1, 2, 3), array(10, 20, 30)),
  (array(1, 2), array(5)),
  (array(), array()),
  (array(1), NULL),
  (NULL, NULL)

-- basic, equal lengths
query
SELECT zip_with(a, b, (x, y) -> x + y) FROM test_zip_with

-- unequal lengths: shorter side padded with NULL
query
SELECT zip_with(a, b, (x, y) -> coalesce(x, 0) + coalesce(y, 0)) FROM test_zip_with

-- build a struct from both elements
query
SELECT zip_with(a, b, (x, y) -> struct(x AS l, y AS r)) FROM test_zip_with

-- all literals
query
SELECT zip_with(array(1, 2), array(3, 4), (x, y) -> x * y)
