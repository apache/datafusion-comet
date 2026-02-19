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
CREATE TABLE test_concat_ws(a string, b string, c string) USING parquet

statement
INSERT INTO test_concat_ws VALUES ('hello', 'beautiful', 'world'), ('', '', ''), (NULL, 'b', 'c'), ('a', NULL, 'c'), (NULL, NULL, NULL)

query
SELECT concat_ws(',', a, b, c) FROM test_concat_ws

query
SELECT concat_ws('', a, b, c) FROM test_concat_ws

query
SELECT concat_ws(NULL, a, b, c) FROM test_concat_ws

-- migrated from CometStringExpressionSuite "string concat_ws"
statement
CREATE TABLE names(id int, first_name varchar(20), middle_initial char(1), last_name varchar(20)) USING parquet

statement
INSERT INTO names VALUES(1, 'James', 'B', 'Taylor'), (2, 'Smith', 'C', 'Davis'), (3, NULL, NULL, NULL), (4, 'Smith', 'C', 'Davis')

query
SELECT concat_ws(' ', first_name, middle_initial, last_name) FROM names

-- literal + literal + literal (falls back to Spark when all args are foldable)
query spark_answer_only
SELECT concat_ws(',', 'hello', 'world'), concat_ws(',', '', ''), concat_ws(',', NULL, 'b', 'c'), concat_ws(NULL, 'a', 'b')
