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
CREATE TABLE test_array_position(int_arr array<int>, str_arr array<string>, val int, str_val string) USING parquet

statement
INSERT INTO test_array_position VALUES
  (array(1, 2, 3, 4), array('a', 'b', 'c'), 2, 'b'),
  (array(1, 2, NULL, 3), array('a', NULL, 'c'), 3, 'c'),
  (array(10, 20, 30), array('x', 'y', 'z'), 99, 'w'),
  (array(), array(), 1, 'a'),
  (NULL, NULL, 1, 'a'),
  (array(1, 1, 1), array('a', 'a', 'a'), 1, 'a')

-- literal args fall back to Spark
query spark_answer_only
SELECT array_position(array(1, 2, 3, 4), 3)

query spark_answer_only
SELECT array_position(array(1, 2, 3, 4), 5)

query spark_answer_only
SELECT array_position(array('a', 'b', 'c'), 'b')

query spark_answer_only
SELECT array_position(array(1, 2, NULL, 3), 3)

query spark_answer_only
SELECT array_position(array(1, 2, 3), cast(NULL as int))

query spark_answer_only
SELECT array_position(cast(NULL as array<int>), 1)

query spark_answer_only
SELECT array_position(array(), 1)

query spark_answer_only
SELECT array_position(array(1, 2, 1, 3), 1)

-- column array + column value
query
SELECT array_position(int_arr, val) FROM test_array_position

-- column array + literal value
query
SELECT array_position(int_arr, 3) FROM test_array_position

-- literal array + column value
query
SELECT array_position(array(1, 2, 3), val) FROM test_array_position

-- string column array + column value
query
SELECT array_position(str_arr, str_val) FROM test_array_position

-- string column array + literal value
query
SELECT array_position(str_arr, 'c') FROM test_array_position

-- expressions in array construction
query
SELECT array_position(array(val, val + 1, val + 2), val) FROM test_array_position
