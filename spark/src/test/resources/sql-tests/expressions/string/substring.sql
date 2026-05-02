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

statement
CREATE TABLE test_substring(s string) USING parquet

statement
INSERT INTO test_substring VALUES ('hello world'), (''), (NULL), ('abc')

query
SELECT substring(s, 1, 5) FROM test_substring

query
SELECT substring(s, -3) FROM test_substring

query
SELECT substring(s, 0, 3) FROM test_substring

query
SELECT substring(s, 1, 0) FROM test_substring

query
SELECT substring(s, 1, -1) FROM test_substring

query
SELECT substring(s, 100) FROM test_substring

query
SELECT substring(s, -2, 3) FROM test_substring

query
SELECT substring(s, -10, 3) FROM test_substring

query
SELECT substring(s, -300, 3) FROM test_substring

-- positive start, no length (two-argument form)
query
SELECT substring(s, 3) FROM test_substring

-- length exceeding string length
query
SELECT substring(s, 1, 100) FROM test_substring

-- start at exact string length boundary
query
SELECT substring(s, 11) FROM test_substring

-- negative start with length that clips before end
query
SELECT substring(s, -2, 1) FROM test_substring

-- negative start equal to string length
query
SELECT substring(s, -11) FROM test_substring

-- very large start
query
SELECT substring(s, 2147483647) FROM test_substring

-- very large length
query
SELECT substring(s, 1, 2147483647) FROM test_substring

-- SUBSTR alias
query
SELECT substr(s, 1, 5) FROM test_substring

query
SELECT substr(s, -3) FROM test_substring

-- SQL standard SUBSTRING(... FROM ... FOR ...) syntax
query
SELECT substring(s FROM 2 FOR 3) FROM test_substring

query
SELECT substring(s FROM -3) FROM test_substring

-- multi-byte UTF-8 characters
statement
CREATE TABLE test_substring_utf8(s string) USING parquet

statement
INSERT INTO test_substring_utf8 VALUES ('こんにちは世界'), ('café'), ('🎉🎊🎈🎁'), ('ab🎉cd'), (NULL)

query
SELECT substring(s, 1, 3) FROM test_substring_utf8

query
SELECT substring(s, 4) FROM test_substring_utf8

query
SELECT substring(s, -2) FROM test_substring_utf8

query
SELECT substring(s, 2, 1) FROM test_substring_utf8

query
SELECT substring(s, -3, 2) FROM test_substring_utf8

-- binary type
statement
CREATE TABLE test_substring_bin(b binary) USING parquet

statement
INSERT INTO test_substring_bin VALUES (X'0102030405'), (X'FF'), (X''), (NULL)

query
SELECT hex(substring(b, 1, 3)) FROM test_substring_bin

query
SELECT hex(substring(b, -2)) FROM test_substring_bin

query
SELECT hex(substring(b, 2, 100)) FROM test_substring_bin

-- substring used in expressions
query
SELECT substring(s, 1, 3) = 'hel' FROM test_substring

query
SELECT length(substring(s, 2)) FROM test_substring

-- scalar string inputs (constant folding is disabled by test framework)
query
SELECT substring('hello world', 1, 5)

query
SELECT substring('hello world', -3)

query
SELECT substring('hello world', 0, 3)

query
SELECT substring('hello world', 1, 0)

query
SELECT substring('hello world', 1, -1)

query
SELECT substring('hello world', 100)

query
SELECT substring('hello world', -2, 3)

query
SELECT substring('hello world', -10, 3)

query
SELECT substring('', 1, 5)

query
SELECT substring(NULL, 1, 5)

-- scalar multi-byte
query
SELECT substring('こんにちは世界', 1, 3)

query
SELECT substring('こんにちは世界', -2)

query
SELECT substring('🎉🎊🎈🎁', 2, 2)

query
SELECT substring('ab🎉cd', 3, 1)

-- scalar with mixed column/literal args
query
SELECT substring(s, 1, 5), substring('hello', 1, 5) FROM test_substring

query
SELECT substring(s, -3), substring('world', -3) FROM test_substring

-- literal + literal + literal
query
SELECT substring('hello world', 1, 5), substring('hello world', -3), substring('', 1, 5), substring(NULL, 1, 5)
