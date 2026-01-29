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
CREATE TABLE test_array_insert(arr array<int>, pos int, val int) USING parquet

statement
INSERT INTO test_array_insert VALUES (array(1, 2, 3), 2, 10), (array(1, 2, 3), 1, 10), (array(1, 2, 3), 4, 10), (array(), 1, 10), (NULL, 1, 10)

query spark_answer_only
SELECT array_insert(arr, pos, val) FROM test_array_insert
