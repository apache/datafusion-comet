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
CREATE TABLE test_str_left(s string, n int) USING parquet

statement
INSERT INTO test_str_left VALUES ('hello', 3), ('hello', 0), ('hello', -1), ('hello', 10), ('', 3), (NULL, 3), ('hello', NULL)

query expect_fallback(Substring pos and len must be literals)
SELECT left(s, n) FROM test_str_left

-- column + literal
query
SELECT left(s, 3) FROM test_str_left

-- column + literal: edge cases
query
SELECT left(s, 0) FROM test_str_left

query
SELECT left(s, -1) FROM test_str_left

query
SELECT left(s, 10) FROM test_str_left

-- literal + column
query expect_fallback(Substring pos and len must be literals)
SELECT left('hello', n) FROM test_str_left

-- literal + literal
query ignore(https://github.com/apache/datafusion-comet/issues/3337)
SELECT left('hello', 3), left('hello', 0), left('hello', -1), left('', 3), left(NULL, 3)

-- unicode
statement
CREATE TABLE test_str_left_unicode(s string) USING parquet

statement
INSERT INTO test_str_left_unicode VALUES ('café'), ('hello世界'), ('😀emoji'), ('తెలుగు'), (NULL)

query
SELECT s, left(s, 2) FROM test_str_left_unicode

query
SELECT s, left(s, 4) FROM test_str_left_unicode

query
SELECT s, left(s, 0) FROM test_str_left_unicode
