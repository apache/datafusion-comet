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

-- Note: Right is a RuntimeReplaceable expression. Spark replaces it with
-- If(IsNull(str), null, If(len <= 0, "", Substring(str, -len, len)))
-- before Comet sees it. CometRight handles the serde, but the optimizer
-- may replace it first. We use spark_answer_only to verify correctness.
statement
CREATE TABLE test_str_right(s string, n int) USING parquet

statement
INSERT INTO test_str_right VALUES ('hello', 3), ('hello', 0), ('hello', -1), ('hello', 10), ('', 3), (NULL, 3), ('hello', NULL)

-- both columns: len must be literal, falls back
query expect_fallback(Substring pos and len must be literals)
SELECT right(s, n) FROM test_str_right

-- column + literal: basic
query
SELECT right(s, 3) FROM test_str_right

-- column + literal: edge cases
query
SELECT right(s, 0) FROM test_str_right

query
SELECT right(s, -1) FROM test_str_right

query
-- n exceeds length of 'hello' (5 chars)
SELECT right(s, 10) FROM test_str_right

-- literal + column: falls back
query expect_fallback(Substring pos and len must be literals)
SELECT right('hello', n) FROM test_str_right

-- literal + literal
query
SELECT right('hello', 3), right('hello', 0), right('hello', -1), right('', 3), right(NULL, 3)

-- null propagation with len <= 0 (critical: NULL str with non-positive len must return NULL, not empty string)
query
SELECT right(CAST(NULL AS STRING), 0), right(CAST(NULL AS STRING), -1), right(CAST(NULL AS STRING), 2)

-- mixed null and non-null values with len <= 0
statement
CREATE TABLE test_str_right_nulls(s string) USING parquet

statement
INSERT INTO test_str_right_nulls VALUES ('hello'), (NULL), (''), ('world')

query
SELECT s, right(s, 0) FROM test_str_right_nulls

query
SELECT s, right(s, -1) FROM test_str_right_nulls

query
SELECT s, right(s, 2) FROM test_str_right_nulls

-- equivalence with substring
query
SELECT s, right(s, 3), substring(s, -3, 3) FROM test_str_right_nulls

-- unicode
statement
CREATE TABLE test_str_right_unicode(s string) USING parquet

statement
INSERT INTO test_str_right_unicode VALUES ('café'), ('hello世界'), ('😀emoji'), ('తెలుగు'), (NULL)

query
SELECT s, right(s, 2) FROM test_str_right_unicode

query
SELECT s, right(s, 4) FROM test_str_right_unicode

query
SELECT s, right(s, 0) FROM test_str_right_unicode
