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

-- Default settings: in-subset literal patterns run on the native Rust path.
-- Routing is asserted in CometRegExpJvmSuite / CometRegexParitySuite; this file
-- only checks result equality with Spark.

statement
CREATE TABLE test_rlike_auto(s string) USING parquet

statement
INSERT INTO test_rlike_auto VALUES ('hello'), ('12345'), (''), (NULL), ('Hello World'), ('abc123'), ('aa'), ('ab'), ('foo'), ('bar'), ('a+b')

query
SELECT s RLIKE 'abc[0-9]+' FROM test_rlike_auto

query
SELECT s RLIKE '[a-zA-Z_][a-zA-Z0-9_]*' FROM test_rlike_auto

query
SELECT s RLIKE '(foo|bar){1,3}' FROM test_rlike_auto

query
SELECT s RLIKE 'a\+b' FROM test_rlike_auto

query
SELECT s RLIKE '' FROM test_rlike_auto

query
SELECT 'hello' RLIKE '[a-z]+', '12345' RLIKE '[0-9]+', '' RLIKE '', NULL RLIKE 'a'
