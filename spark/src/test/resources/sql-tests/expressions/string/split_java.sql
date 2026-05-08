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

-- Test split via JVM regex engine (default engine)

statement
CREATE TABLE test_split_java(s string) USING parquet

statement
INSERT INTO test_split_java VALUES ('one,two,three'), ('hello'), (''), (NULL), ('a::b::c'), ('aXbXc')

-- basic split on comma
query
SELECT split(s, ',', -1) FROM test_split_java

-- split with limit
query
SELECT split(s, ',', 2) FROM test_split_java

-- split on regex pattern
query
SELECT split(s, '[,:]', -1) FROM test_split_java

-- split on multi-char separator
query
SELECT split(s, '::', -1) FROM test_split_java

-- lookahead in pattern (Java-only)
query
SELECT split(s, '(?=X)', -1) FROM test_split_java

-- embedded flags (Java-only)
query
SELECT split(s, '(?i)x', -1) FROM test_split_java

-- literal arguments
query
SELECT split('a,b,c', ',', -1), split('hello', ',', -1), split(NULL, ',', -1)
