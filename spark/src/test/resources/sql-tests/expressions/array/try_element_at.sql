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

-- try_element_at lowers to ElementAt(failOnError=false), so out-of-bounds returns NULL.

statement
CREATE TABLE test_try_element_at(arr array<int>, m map<string, int>) USING parquet

statement
INSERT INTO test_try_element_at VALUES
  (array(10, 20, 30), map('a', 1, 'b', 2)),
  (array(99), map('x', 99)),
  (NULL, NULL)

-- array input: in-bounds access
query
SELECT try_element_at(arr, 1) FROM test_try_element_at

-- array input: last element via negative index
query
SELECT try_element_at(arr, -1) FROM test_try_element_at

-- array input: out-of-bounds returns NULL (no exception)
query
SELECT try_element_at(arr, 100) FROM test_try_element_at

-- NULL array input
query
SELECT try_element_at(CAST(NULL AS ARRAY<INT>), 1)

-- literal array arguments: same codegen bug as element_at with literal arrays
query ignore(Spark codegen bug with literal element_at when constant folding is disabled)
SELECT try_element_at(array(10, 20, 30), 1), try_element_at(array(10, 20, 30), 99)

-- map input falls back to Spark
query spark_answer_only
SELECT try_element_at(m, 'a'), try_element_at(m, 'missing') FROM test_try_element_at
