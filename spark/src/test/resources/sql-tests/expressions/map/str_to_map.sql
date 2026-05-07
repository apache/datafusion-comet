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

-- Tests for Spark-compatible str_to_map function
-- https://spark.apache.org/docs/latest/api/sql/index.html#str_to_map
--
-- Test cases derived from Spark test("StringToMap"):
-- https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ComplexTypeSuite.scala#L525-L618

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_str_to_map(
  s STRING,
  pair_delim STRING,
  key_value_delim STRING,
  s_char CHAR(16),
  pair_delim_char CHAR(4),
  key_value_delim_char CHAR(4),
  s_varchar VARCHAR(16),
  pair_delim_varchar VARCHAR(4),
  key_value_delim_varchar VARCHAR(4)
) USING parquet

statement
INSERT INTO test_str_to_map VALUES
  ('a:1,b:2,c:3', ',', ':', 'a:1,b:2,c:3', ',', ':', 'a:1,b:2,c:3', ',', ':'),
  ('x=9;y=8', ';', '=', 'x=9;y=8', ';', '=', 'x=9;y=8', ';', '='),
  (NULL, ',', ':', NULL, ',', ':', NULL, ',', ':')

-- s0: Basic test with default delimiters
query
SELECT str_to_map('a:1,b:2,c:3')

-- s1: Preserve spaces in values
query
SELECT str_to_map('a: ,b:2')

-- s2: Custom key-value delimiter '='
query
SELECT str_to_map('a=1,b=2,c=3', ',', '=')

-- s3: Empty string returns map with empty key and NULL value
query
SELECT str_to_map('', ',', '=')

-- s4: Custom pair delimiter '_'
query
SELECT str_to_map('a:1_b:2_c:3', '_', ':')

-- s5: Single key without value returns NULL value
query
SELECT str_to_map('a')

-- s6: Custom delimiters '&' and '='
query
SELECT str_to_map('a=1&b=2&c=3', '&', '=')

-- Duplicate keys: EXCEPTION policy (Spark 3.0+ default)
-- TODO: Add LAST_WIN policy tests when spark.sql.mapKeyDedupPolicy config is supported
-- query
-- SELECT str_to_map('a:1,b:2,a:3')

-- NULL input returns NULL
query
SELECT str_to_map(NULL, ',', ':')

-- Explicit 3-arg form
query
SELECT str_to_map('a:1,b:2,c:3', ',', ':')

-- Missing key-value delimiter results in NULL value
query
SELECT str_to_map('a,b:2', ',', ':')

-- Multi-row test
query
SELECT str_to_map(s) FROM test_str_to_map

-- Rows with per-row delimiters
query
SELECT str_to_map(s, pair_delim, key_value_delim) FROM test_str_to_map

-- STRING input with literal delimiters
query
SELECT str_to_map(s, ',', ':') FROM test_str_to_map

-- CHAR input and delimiters with per-row delimiter values
query
SELECT str_to_map(s_char, pair_delim_char, key_value_delim_char) FROM test_str_to_map

-- VARCHAR input and delimiters with per-row delimiter values
query
SELECT str_to_map(s_varchar, pair_delim_varchar, key_value_delim_varchar) FROM test_str_to_map
