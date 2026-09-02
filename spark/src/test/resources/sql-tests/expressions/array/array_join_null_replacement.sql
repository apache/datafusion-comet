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

-- Regression coverage for #3178. Spark returns null whenever nullReplacement is null, even for
-- an array with no nulls to replace, while array_to_string reads a null null_string as "omit
-- nulls".

statement
CREATE TABLE test_aj_nullrep(arr array<string>, delim string, nullrep string) USING parquet

statement
INSERT INTO test_aj_nullrep VALUES
  (array('a', NULL, 'c'), ',', NULL),
  (array('a', 'b', 'c'), ',', NULL),
  (array(NULL, NULL), ',', NULL),
  (NULL, ',', NULL),
  (array('a', NULL, 'c'), ',', 'X')

-- null replacement as a column, mixed with a non-null replacement row
query
SELECT array_join(arr, delim, nullrep) FROM test_aj_nullrep

-- null replacement as a literal, array containing nulls
query
SELECT array_join(array('a', NULL, 'b'), ',', cast(NULL as string))

-- null replacement as a literal, array containing no nulls at all
query
SELECT array_join(array('a', 'b'), ',', cast(NULL as string))

-- a non-nullable literal replacement takes the unwrapped path
query
SELECT array_join(array('a', NULL, 'b'), ',', 'X')
