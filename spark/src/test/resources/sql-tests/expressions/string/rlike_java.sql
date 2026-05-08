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

-- Test RLIKE via JVM regex engine (default engine)

statement
CREATE TABLE test_rlike_java(s string) USING parquet

statement
INSERT INTO test_rlike_java VALUES ('hello'), ('12345'), (''), (NULL), ('Hello World'), ('abc123'), ('aa'), ('ab')

query
SELECT s RLIKE '^\d+$' FROM test_rlike_java

query
SELECT s RLIKE '^[a-z]+$' FROM test_rlike_java

query
SELECT s RLIKE '' FROM test_rlike_java

-- backreference (Java-only)
query
SELECT s RLIKE '^(\w)\1$' FROM test_rlike_java

-- lookahead (Java-only)
query
SELECT s RLIKE 'abc(?=\d)' FROM test_rlike_java

-- embedded flags (Java-only)
query
SELECT s RLIKE '(?i)hello' FROM test_rlike_java

-- literal arguments
query
SELECT 'hello' RLIKE '^[a-z]+$', '12345' RLIKE '^\d+$', '' RLIKE '', NULL RLIKE 'a'
