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

-- ===== INT arrays (native array_reverse) =====

statement
CREATE TABLE test_array_reverse_int(arr array<int>) USING parquet

statement
INSERT INTO test_array_reverse_int VALUES
  (array(1, 2, 3)),
  (array(1, NULL, 3)),
  (array()),
  (NULL)

query
SELECT reverse(arr) FROM test_array_reverse_int

-- ===== STRING arrays =====

statement
CREATE TABLE test_array_reverse_string(arr array<string>) USING parquet

statement
INSERT INTO test_array_reverse_string VALUES
  (array('a', 'b', 'c')),
  (array('a', NULL, 'c')),
  (array()),
  (NULL)

query
SELECT reverse(arr) FROM test_array_reverse_string

-- ===== STRUCT arrays =====
-- Native array_reverse cannot handle struct element types, so getSupportLevel reports
-- Incompatible and Comet routes these through the JVM codegen dispatcher, staying native instead
-- of silently falling back to Spark.

statement
CREATE TABLE test_array_reverse_struct(c1 struct<a:int, b:string>, c2 struct<a:int, b:string>)
USING parquet

statement
INSERT INTO test_array_reverse_struct VALUES
  (named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y')),
  (named_struct('a', 3, 'b', NULL), named_struct('a', 4, 'b', 'z')),
  (NULL, named_struct('a', 5, 'b', 'w'))

query
SELECT reverse(array(c1, c2)) FROM test_array_reverse_struct

query
SELECT reverse(array(array(c1), array(c2))) FROM test_array_reverse_struct

-- literal arguments (constant folding is disabled by the suite, so these run natively)
query
SELECT
  reverse(array(1, 2, 3)),
  reverse(array('a', 'b', 'c')),
  reverse(array(named_struct('a', 1), named_struct('a', 2))),
  reverse(cast(NULL as array<int>))
