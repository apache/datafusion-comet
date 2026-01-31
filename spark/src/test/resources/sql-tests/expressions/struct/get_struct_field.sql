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
CREATE TABLE test_struct(s struct<name: string, age: int, score: double>) USING parquet

statement
INSERT INTO test_struct VALUES (named_struct('name', 'Alice', 'age', 30, 'score', 95.5)), (named_struct('name', 'Bob', 'age', 25, 'score', 88.0)), (NULL)

query
SELECT s.name, s.age, s.score FROM test_struct

query
SELECT s.name, s.age + 1, s.score * 2 FROM test_struct
