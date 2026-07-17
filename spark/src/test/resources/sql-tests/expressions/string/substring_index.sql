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
CREATE TABLE test_substring_index(s string, delim string, cnt int) USING parquet

statement
INSERT INTO test_substring_index VALUES
  ('www.apache.org', '.', 1),
  ('www.apache.org', '.', 2),
  ('www.apache.org', '.', 3),
  ('www.apache.org', '.', -1),
  ('www.apache.org', '.', -2),
  ('www.apache.org', '.', -3),
  ('www.apache.org', '.', 0),
  ('hello', '.', 1),
  ('', '.', 1),
  ('www.apache.org', '', 1),
  (NULL, '.', 1),
  ('www.apache.org', NULL, 1),
  ('www.apache.org', '.', NULL)

-- all columns
query
SELECT substring_index(s, delim, cnt) FROM test_substring_index

-- literal arguments
query
SELECT substring_index('www.apache.org', '.', 1),
       substring_index('www.apache.org', '.', 2),
       substring_index('www.apache.org', '.', -1),
       substring_index('www.apache.org', '.', -2),
       substring_index('www.apache.org', '.', 0)

-- NULL literal arguments
query
SELECT substring_index(NULL, '.', 1),
       substring_index('www.apache.org', NULL, 1),
       substring_index('www.apache.org', '.', NULL)

-- column string, literal delimiter and count
query
SELECT substring_index(s, '.', 1) FROM test_substring_index

-- literal string, column delimiter and count
query
SELECT substring_index('www.apache.org', delim, cnt) FROM test_substring_index

-- count exceeds number of delimiters (returns full string)
query
SELECT substring_index('www.apache.org', '.', 10),
       substring_index('www.apache.org', '.', -10)

-- multi-character delimiter
query
SELECT substring_index('one::two::three', '::', 1),
       substring_index('one::two::three', '::', 2),
       substring_index('one::two::three', '::', -1),
       substring_index('one::two::three', '::', -2)

-- delimiter not found
query
SELECT substring_index('hello world', 'xyz', 1),
       substring_index('hello world', 'xyz', -1)

-- empty string input
query
SELECT substring_index('', '.', 1),
       substring_index('', '.', -1)

-- empty delimiter
query
SELECT substring_index('www.apache.org', '', 1),
       substring_index('www.apache.org', '', -1)

-- multibyte UTF-8 characters
query
SELECT substring_index('a.b.c', '.', 2),
       substring_index('中文.测试.数据', '.', 1),
       substring_index('中文.测试.数据', '.', -1),
       substring_index('中文.测试.数据', '.', 2)

-- delimiter at start of string
query
SELECT substring_index('.www.apache.org', '.', 1),
       substring_index('.www.apache.org', '.', 2),
       substring_index('.www.apache.org', '.', -1)

-- delimiter at end of string
query
SELECT substring_index('www.apache.org.', '.', -1),
       substring_index('www.apache.org.', '.', 3),
       substring_index('www.apache.org.', '.', -2)

-- delimiter equals the full string
query
SELECT substring_index('abc', 'abc', 1),
       substring_index('abc', 'abc', -1)

-- large count values
query
SELECT substring_index('www.apache.org', '.', 2147483647),
       substring_index('www.apache.org', '.', -2147483647)
