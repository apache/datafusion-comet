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
CREATE TABLE test_array_contains(arr array<int>, val int) USING parquet

statement
INSERT INTO test_array_contains VALUES (array(1, 2, 3), 2), (array(1, 2, 3), 4), (array(1, NULL, 3), NULL), (array(), 1), (NULL, 1)

query spark_answer_only
SELECT array_contains(arr, val) FROM test_array_contains

-- column + literal
query ignore(https://github.com/apache/datafusion-comet/issues/3346)
SELECT array_contains(arr, 2) FROM test_array_contains

-- literal + column
query spark_answer_only
SELECT array_contains(array(1, 2, 3), val) FROM test_array_contains

-- literal + literal
-- Note: array_contains(array(), 1) still has a bug (issue #3346) so we use spark_answer_only
-- The NULL array case (cast(NULL as array<int>)) was fixed in issue #3345
query spark_answer_only
SELECT array_contains(array(1, 2, 3), 2), array_contains(array(1, 2, 3), 4), array_contains(array(), 1), array_contains(cast(NULL as array<int>), 1)

-- Additional NULL array tests (issue #3345 fix verification)
-- NULL array with integer value
query
SELECT array_contains(cast(NULL as array<int>), 1)

-- NULL array with string value
query
SELECT array_contains(cast(NULL as array<string>), 'test')

-- NULL array with NULL value
query
SELECT array_contains(cast(NULL as array<int>), cast(NULL as int))

-- NULL array with column value
query
SELECT array_contains(cast(NULL as array<int>), val) FROM test_array_contains
