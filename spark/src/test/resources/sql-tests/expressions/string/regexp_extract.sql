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

-- Test regexp_extract via JVM regex engine
-- Raw literals keep Spark from dropping regex backslashes such as '\d'.

statement
CREATE TABLE test_regexp_extract(s string) USING parquet

statement
INSERT INTO test_regexp_extract VALUES ('abc123def'), ('no match'), (NULL), ('xyz789'), ('hello world'), ('aa')

-- group 0: entire match
query expect_dispatch(regexp_extract)
SELECT regexp_extract(s, r'\d+', 0) FROM test_regexp_extract

-- group 1: first capturing group
query expect_dispatch(regexp_extract)
SELECT regexp_extract(s, r'([a-z]+)(\d+)', 1) FROM test_regexp_extract

-- group 2: second capturing group
query expect_dispatch(regexp_extract)
SELECT regexp_extract(s, r'([a-z]+)(\d+)', 2) FROM test_regexp_extract

-- no match returns empty string
query expect_dispatch(regexp_extract)
SELECT regexp_extract(s, 'NOMATCH', 0) FROM test_regexp_extract

-- backreference pattern (Java-only)
query expect_dispatch(regexp_extract)
SELECT regexp_extract(s, r'(\w)\1', 0) FROM test_regexp_extract

-- lookahead (Java-only)
query expect_dispatch(regexp_extract)
SELECT regexp_extract(s, r'abc(?=\d)', 0) FROM test_regexp_extract

-- embedded flags (Java-only)
query expect_dispatch(regexp_extract)
SELECT regexp_extract(s, '(?i)HELLO', 0) FROM test_regexp_extract

-- literal arguments
query expect_dispatch(regexp_extract)
SELECT regexp_extract('abc123', r'(\d+)', 1), regexp_extract('no digits', r'(\d+)', 1), regexp_extract(NULL, r'(\d+)', 1)
