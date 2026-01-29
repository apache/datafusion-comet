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
CREATE TABLE test_ends_with(s string, suffix string) USING parquet

statement
INSERT INTO test_ends_with VALUES ('hello world', 'world'), ('hello', ''), ('', ''), ('hello', 'xyz'), (NULL, 'a'), ('hello', NULL)

query
SELECT endswith(s, suffix) FROM test_ends_with

-- column + literal
query
SELECT endswith(s, 'world') FROM test_ends_with

-- literal + column
query
SELECT endswith('hello world', suffix) FROM test_ends_with

-- literal + literal
query
SELECT endswith('hello world', 'world'), endswith('hello', 'xyz'), endswith('', ''), endswith(NULL, 'a')
