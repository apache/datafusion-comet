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
CREATE TABLE test_trim(s string) USING parquet

statement
INSERT INTO test_trim VALUES ('  hello  '), ('hello'), (''), (NULL), ('  '), (' hello world ')

query
SELECT trim(s), ltrim(s), rtrim(s) FROM test_trim

query
SELECT trim(BOTH 'h' FROM s) FROM test_trim

-- literal arguments
query
SELECT trim('  hello  '), ltrim('  hello  '), rtrim('  hello  ')

query
SELECT trim(BOTH 'h' FROM 'hello'), trim(LEADING ' ' FROM '  hello  '), trim(TRAILING ' ' FROM '  hello  ')
