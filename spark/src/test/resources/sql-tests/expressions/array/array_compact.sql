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
CREATE TABLE test_array_compact(
  ints array<int>,
  strs array<string>,
  dbls array<double>,
  nested array<array<int>>
) USING parquet

statement
INSERT INTO test_array_compact VALUES
  (array(1, NULL, 2, NULL, 3), array('a', NULL, 'b', NULL, 'c'), array(1.0, NULL, 2.0), array(array(1, NULL, 3), NULL, array(4, NULL, 6))),
  (array(), array(), array(), array()),
  (NULL, NULL, NULL, NULL),
  (array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL)),
  (array(1, 2, 3), array('x', 'y', 'z'), array(1.5, 2.5), array(array(1, 2), array(3, 4)))

-- integer column
query
SELECT array_compact(ints) FROM test_array_compact

-- string column
query
SELECT array_compact(strs) FROM test_array_compact

-- double column
query
SELECT array_compact(dbls) FROM test_array_compact

-- nested array column: outer nulls removed, inner nulls preserved
query
SELECT array_compact(nested) FROM test_array_compact

-- literal arguments
query
SELECT array_compact(array(1, NULL, 2, NULL, 3))

-- literal string array
query
SELECT array_compact(array('a', NULL, 'b'))

-- all-null literal array
query
SELECT array_compact(array(NULL, NULL, NULL))
