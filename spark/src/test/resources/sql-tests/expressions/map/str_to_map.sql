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

-- s0: Basic test with default delimiters
query spark_answer_only
SELECT str_to_map('a:1,b:2,c:3')

-- s1: Preserve spaces in values
query spark_answer_only
SELECT str_to_map('a: ,b:2')

-- s2: Custom key-value delimiter '='
query spark_answer_only
SELECT str_to_map('a=1,b=2,c=3', ',', '=')

-- s3: Empty string returns map with empty key and NULL value
query spark_answer_only
SELECT str_to_map('', ',', '=')

-- s4: Custom pair delimiter '_'
query spark_answer_only
SELECT str_to_map('a:1_b:2_c:3', '_', ':')

-- s5: Single key without value returns NULL value
query spark_answer_only
SELECT str_to_map('a')

-- s6: Custom delimiters '&' and '='
query spark_answer_only
SELECT str_to_map('a=1&b=2&c=3', '&', '=')

-- Duplicate keys: EXCEPTION policy (Spark 3.0+ default)
-- TODO: Add LAST_WIN policy tests when spark.sql.mapKeyDedupPolicy config is supported
-- query spark_answer_only
-- SELECT str_to_map('a:1,b:2,a:3')

-- NULL input returns NULL
query spark_answer_only
SELECT str_to_map(NULL, ',', ':')

-- Explicit 3-arg form
query spark_answer_only
SELECT str_to_map('a:1,b:2,c:3', ',', ':')

-- Missing key-value delimiter results in NULL value
query spark_answer_only
SELECT str_to_map('a,b:2', ',', ':')

-- Multi-row test
query spark_answer_only
SELECT str_to_map(col) FROM (VALUES ('a:1,b:2'), ('x:9'), (NULL)) AS t(col)

-- Multi-row with custom delimiter
query spark_answer_only
SELECT str_to_map(col, ',', '=') FROM (VALUES ('a=1,b=2'), ('x=9'), (NULL)) AS t(col)

-- Per-row delimiters: each row can have different delimiters
query spark_answer_only
SELECT str_to_map(col1, col2, col3) FROM (VALUES ('a=1,b=2', ',', '='), ('x#9', ',', '#'), (NULL, ',', '=')) AS t(col1, col2, col3)
