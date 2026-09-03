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

-- Regression coverage for #3178: Spark returns null whenever nullReplacement is null, even for
-- an array with no nulls to replace, while array_to_string reads a null null_string as "omit
-- nulls". The replacement is a column so these take the guarded native path.

statement
CREATE TABLE test_aj_nullrep(arr array<string>, delim string, nullrep string) USING parquet

statement
INSERT INTO test_aj_nullrep VALUES
  (array('a', NULL, 'c'), ',', NULL),
  (array('a', 'b', 'c'), ',', NULL),
  (array(NULL, NULL), ',', NULL),
  (NULL, ',', NULL),
  (array('a', NULL, 'c'), ',', 'X'),
  (array('a', NULL, 'c'), ',', '')

query
SELECT array_join(arr, delim, nullrep) FROM test_aj_nullrep

query
SELECT array_join(arr, ',', nullrep) FROM test_aj_nullrep

-- a non-nullable literal replacement takes the unguarded path
query
SELECT array_join(array('a', NULL, 'b'), ',', 'X')
