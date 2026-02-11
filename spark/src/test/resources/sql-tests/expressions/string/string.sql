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

-- substring with start < 1
statement
CREATE TABLE t(col string) USING parquet

statement
INSERT INTO t VALUES('123456')

query
SELECT substring(col, 0) FROM t

query
SELECT substring(col, -1) FROM t

-- md5
statement
CREATE TABLE test_md5(col String) USING parquet

statement
INSERT INTO test_md5 VALUES ('test1'), ('test1'), ('test2'), ('test2'), (NULL), ('')

query
SELECT md5(col) FROM test_md5

-- unhex
statement
CREATE TABLE unhex_table(col string) USING parquet

statement
INSERT INTO unhex_table VALUES ('537061726B2053514C'), ('737472696E67'), ('\0'), (''), ('###'), ('G123'), ('hello'), ('A1B'), ('0A1B')

query
SELECT unhex(col) FROM unhex_table
