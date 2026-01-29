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

-- Setup
statement
CREATE TABLE test(col1 int, col2 int) USING parquet

statement
INSERT INTO test VALUES(1111, 2)

statement
INSERT INTO test VALUES(1111, 2)

statement
INSERT INTO test VALUES(3333, 4)

statement
INSERT INTO test VALUES(5555, 6)

-- Queries
query
SELECT col1 & col2, col1 | col2, col1 ^ col2 FROM test

query
SELECT col1 & 1234, col1 | 1234, col1 ^ 1234 FROM test

query
SELECT shiftright(col1, 2), shiftright(col1, col2) FROM test

query
SELECT shiftleft(col1, 2), shiftleft(col1, col2) FROM test

query
SELECT ~(11), ~col1, ~col2 FROM test
