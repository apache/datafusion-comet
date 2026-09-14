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

-- Routes find_in_set through the codegen dispatcher so behavior matches Spark exactly.

statement
CREATE TABLE test_find_in_set(s string) USING parquet

statement
INSERT INTO test_find_in_set VALUES ('b'), ('a'), ('z'), (NULL)

query
SELECT s, find_in_set(s, 'a,b,c') FROM test_find_in_set

-- literal arguments
query
SELECT find_in_set('b', 'a,b,c'), find_in_set('x', 'a,b,c'), find_in_set('', 'a,b,c')
