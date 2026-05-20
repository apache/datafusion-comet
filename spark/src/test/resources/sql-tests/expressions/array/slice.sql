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
CREATE TABLE test_slice(arr array<int>, s int, l int) USING parquet

statement
INSERT INTO test_slice VALUES
  (array(1, 2, 3, 4, 5), 2, 3),
  (array(1, 2, 3, 4, 5), 1, 5),
  (array(1, 2, 3, 4, 5), 1, 10),
  (array(1, 2, 3, 4, 5), 3, 0),
  (array(1, 2, 3, 4, 5), -2, 2),
  (array(1, 2, 3, 4, 5), -10, 2),
  (array(1, 2, 3, 4, 5), 10, 2),
  (array(1, NULL, 3, NULL, 5), 1, 5),
  (array(), 1, 3),
  (NULL, 1, 3),
  (array(1, 2, 3), NULL, 2),
  (array(1, 2, 3), 1, NULL)

-- column array, column start, column length
query
SELECT slice(arr, s, l) FROM test_slice

-- column array, literal start and length
query
SELECT slice(arr, 2, 2) FROM test_slice

-- column array, negative literal start
query
SELECT slice(arr, -1, 1) FROM test_slice

-- string element type
statement
CREATE TABLE test_slice_string(arr array<string>) USING parquet

statement
INSERT INTO test_slice_string VALUES
  (array('a', 'b', 'c', 'd')),
  (array('é', '日本', '', 'x')),
  (array('a', NULL, 'c')),
  (NULL)

query
SELECT slice(arr, 1, 2) FROM test_slice_string
