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
CREATE TABLE test_str_replace(s string, search string, replace string) USING parquet

statement
INSERT INTO test_str_replace VALUES ('hello world', 'world', 'there'), ('aaa', 'a', 'bb'), ('hello', 'xyz', 'abc'), ('', 'a', 'b'), (NULL, 'a', 'b'), ('hello', '', 'x'), ('aaaa', 'aa', 'x'), ('你好你好', '你好', 'X'), ('😀a😀', '😀', 'x')

query
SELECT replace(s, search, replace) FROM test_str_replace

-- Empty literal search: DataFusion's replace diverges from Spark
-- (Spark short-circuits and returns the source unchanged). The custom
-- CometStringReplace serde routes through the codegen dispatcher so
-- Spark's own doGenCode handles this case.
-- https://github.com/apache/datafusion-comet/issues/4497
query
SELECT replace('hello', '', 'x')

query
SELECT replace('', '', 'x')

query
SELECT replace(NULL, '', 'x')

query
SELECT replace('hello', '', NULL)

-- Overlapping candidates: Spark replaces non-overlapping left-to-right
-- ('aaaa' + 'aa' -> 'xx'). Compatibility coverage for overlapping candidates.
query
SELECT replace(s, 'aa', 'x') FROM test_str_replace WHERE s = 'aaaa'

-- Multi-byte UTF-8 values. Compatibility coverage for multi-byte UTF-8 values.
query
SELECT replace(s, '你好', 'X') FROM test_str_replace WHERE s = '你好你好'

query
SELECT replace(s, '😀', 'x') FROM test_str_replace WHERE s = '😀a😀'

-- Multi-byte replacement.
query
SELECT replace(s, '你好', '世界') FROM test_str_replace WHERE s = '你好你好'

-- Replacement contains the search string; replacement is not applied recursively.
query
SELECT replace(s, 'aa', 'aaa') FROM test_str_replace WHERE s = 'aaaa'

-- column + literal + literal
query
SELECT replace(s, 'world', 'there') FROM test_str_replace

-- literal + column + column
query
SELECT replace('hello world', search, replace) FROM test_str_replace

-- literal + literal + literal
query
SELECT replace('hello world', 'world', 'there'), replace('aaa', 'a', 'bb'), replace('hello', 'xyz', 'abc'), replace(NULL, 'a', 'b')
