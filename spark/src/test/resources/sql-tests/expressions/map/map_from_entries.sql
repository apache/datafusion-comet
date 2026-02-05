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
CREATE TABLE test_map_from_entries(entries array<struct<key:string, value:int>>) USING parquet

statement
INSERT INTO test_map_from_entries VALUES (array(struct('a', 1), struct('b', 2), struct('c', 3))), (array()), (NULL)

query
SELECT map_from_entries(entries) FROM test_map_from_entries

query expect_fallback(Using BinaryType as Map keys is not allowed in map_from_entries)
SELECT map_from_entries(array(struct(cast('x' as binary), 10)))

query expect_fallback(Using BinaryType as Map values is not allowed in map_from_entries)
SELECT map_from_entries(array(struct(10, cast('x' as binary))))

-- literal arguments
query spark_answer_only
SELECT map_from_entries(array(struct('x', 10), struct('y', 20), struct('z', 30)))
