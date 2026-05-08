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

-- Test regexp_instr via JVM regex engine (default engine)

statement
CREATE TABLE test_regexp_instr(s string) USING parquet

statement
INSERT INTO test_regexp_instr VALUES ('abc123def'), ('no match'), (NULL), ('123xyz'), ('hello world'), ('aa')

-- basic: position of first digit sequence
query
SELECT regexp_instr(s, '\d+', 0) FROM test_regexp_instr

-- group 1 (still returns position of entire match per Spark semantics)
query
SELECT regexp_instr(s, '([a-z]+)(\d+)', 1) FROM test_regexp_instr

-- no match returns 0
query
SELECT regexp_instr(s, 'NOMATCH', 0) FROM test_regexp_instr

-- backreference pattern (Java-only)
query
SELECT regexp_instr(s, '(\w)\1', 0) FROM test_regexp_instr

-- embedded flags (Java-only)
query
SELECT regexp_instr(s, '(?i)HELLO', 0) FROM test_regexp_instr

-- literal arguments
query
SELECT regexp_instr('abc123', '\d+', 0), regexp_instr('no digits', '\d+', 0), regexp_instr(NULL, '\d+', 0)
