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

-- Test regexp_extract_all via JVM regex engine (default engine)

statement
CREATE TABLE test_regexp_extract_all(s string) USING parquet

statement
INSERT INTO test_regexp_extract_all VALUES ('abc123def456'), ('no match'), (NULL), ('100-200-300'), ('hello world')

-- group 0: all entire matches
query
SELECT regexp_extract_all(s, '\d+', 0) FROM test_regexp_extract_all

-- group 1: first capturing group from each match
query
SELECT regexp_extract_all(s, '([a-z]+)(\d+)', 1) FROM test_regexp_extract_all

-- group 2: second capturing group from each match
query
SELECT regexp_extract_all(s, '([a-z]+)(\d+)', 2) FROM test_regexp_extract_all

-- no match returns empty array
query
SELECT regexp_extract_all(s, 'NOMATCH', 0) FROM test_regexp_extract_all

-- backreference pattern (Java-only)
query
SELECT regexp_extract_all(s, '(\d)\1', 0) FROM test_regexp_extract_all

-- embedded flags (Java-only)
query
SELECT regexp_extract_all(s, '(?i)[A-Z]+', 0) FROM test_regexp_extract_all

-- literal arguments
query
SELECT regexp_extract_all('abc123def456', '(\d+)', 1), regexp_extract_all('no digits', '(\d+)', 1), regexp_extract_all(NULL, '(\d+)', 1)
