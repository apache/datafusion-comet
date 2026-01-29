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

-- compare true/false to negative zero
statement
CREATE TABLE test(col1 boolean, col2 float) USING parquet

statement
INSERT INTO test VALUES(true, -0.0)

statement
INSERT INTO test VALUES(false, -0.0)

query
SELECT col1, negative(col2), cast(col1 as float), col1 = negative(col2) FROM test

-- not
statement
CREATE TABLE test_not(col1 int, col2 boolean) USING parquet

statement
INSERT INTO test_not VALUES(1, false), (2, true), (3, true), (3, false)

query
SELECT col1, col2, NOT(col2), !(col2) FROM test_not
