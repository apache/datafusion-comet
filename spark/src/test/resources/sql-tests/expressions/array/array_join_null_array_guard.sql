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

-- Spark short-circuits on a null array before evaluating the delimiter, while DataFusion
-- evaluates every argument eagerly. element_at is 1-based, so delims[0] throws whenever it is
-- evaluated: the arr IS NULL row must not reach it (#3178).

statement
CREATE TABLE test_aj_guard(arr array<string>, delims array<string>, idx int) USING parquet

statement
INSERT INTO test_aj_guard VALUES
  (NULL, array(','), 0),
  (array('a', 'b'), array(','), 1),
  (array('a', NULL, 'b'), array(';'), 1)

-- the null-array row short-circuits before element_at(delims, 0) can throw
query
SELECT array_join(arr, element_at(delims, idx)) FROM test_aj_guard WHERE arr IS NULL

-- rows with a real index still join normally
query
SELECT array_join(arr, element_at(delims, idx)) FROM test_aj_guard WHERE arr IS NOT NULL

-- and the same shape with a null replacement
query
SELECT array_join(arr, element_at(delims, idx), 'X') FROM test_aj_guard WHERE arr IS NULL
