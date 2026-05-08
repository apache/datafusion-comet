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

-- Test regexp_extract via JVM regex engine (default engine)

statement
CREATE TABLE test_regexp_extract(s string) USING parquet

statement
INSERT INTO test_regexp_extract VALUES ('abc123def'), ('no match'), (NULL), ('xyz789'), ('hello world'), ('aa')

-- group 0: entire match
query
SELECT regexp_extract(s, '\d+', 0) FROM test_regexp_extract

-- group 1: first capturing group
query
SELECT regexp_extract(s, '([a-z]+)(\d+)', 1) FROM test_regexp_extract

-- group 2: second capturing group
query
SELECT regexp_extract(s, '([a-z]+)(\d+)', 2) FROM test_regexp_extract

-- no match returns empty string
query
SELECT regexp_extract(s, 'NOMATCH', 0) FROM test_regexp_extract

-- backreference pattern (Java-only)
query
SELECT regexp_extract(s, '(\w)\1', 0) FROM test_regexp_extract

-- lookahead (Java-only)
query
SELECT regexp_extract(s, 'abc(?=\d)', 0) FROM test_regexp_extract

-- embedded flags (Java-only)
query
SELECT regexp_extract(s, '(?i)HELLO', 0) FROM test_regexp_extract

-- literal arguments
query
SELECT regexp_extract('abc123', '(\d+)', 1), regexp_extract('no digits', '(\d+)', 1), regexp_extract(NULL, '(\d+)', 1)
