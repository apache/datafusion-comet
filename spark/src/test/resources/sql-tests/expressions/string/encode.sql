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

-- Tests for the SQL `encode(str, charset)` function (StringType, StringType) -> BinaryType.
--
-- `encode` runs through the codegen dispatcher (Spark's own doGenCode inside the Comet
-- pipeline) so behavior matches Spark exactly across all supported charsets and across the
-- Spark 4.0 `legacyCharsets` / `legacyErrorAction` modes. This is the dual of the `decode`
-- codegen-dispatch path (#4465).
--
-- Each `query` block runs checkSparkAnswerAndOperator, which fails if the expression fell back
-- to Spark instead of executing natively, so these are non-vacuous.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_encode(s string) USING parquet

statement
INSERT INTO test_encode VALUES ('hello'), ('world'), (''), ('café'), (NULL)

-- Charset form over multiple charsets

query
SELECT encode(s, 'utf-8') FROM test_encode

query
SELECT encode(s, 'UTF-8') FROM test_encode

query
SELECT encode(s, 'UTF-16') FROM test_encode

query
SELECT encode(s, 'UTF-16BE') FROM test_encode

query
SELECT encode(s, 'UTF-16LE') FROM test_encode

query
SELECT encode(s, 'ISO-8859-1') FROM test_encode

-- US-ASCII: use ASCII-only input. Non-ASCII input under US-ASCII throws on Spark 4.0+ strict
-- mode (matching Spark), so it is not exercised here.

statement
CREATE TABLE test_encode_ascii(s string) USING parquet

statement
INSERT INTO test_encode_ascii VALUES ('hello'), ('world'), (''), (NULL)

query
SELECT encode(s, 'US-ASCII') FROM test_encode_ascii

-- Literal inputs including NULL and empty string

query
SELECT encode('hello', 'utf-8'), encode('', 'utf-8'), encode(CAST(NULL AS STRING), 'utf-8')

-- Non-literal charset column: the charset is an ordinary child expression, not required to be
-- foldable. The dispatcher handles it because it runs Spark's own code.

statement
CREATE TABLE test_encode_charset(s string, cs string) USING parquet

statement
INSERT INTO test_encode_charset VALUES ('hello', 'utf-8'), ('world', 'UTF-16BE'), ('café', 'ISO-8859-1'), (NULL, 'utf-8')

query
SELECT encode(s, cs) FROM test_encode_charset

-- Round-trip: decode(encode(x)) recovers the original string

query
SELECT decode(encode(s, 'UTF-8'), 'UTF-8') FROM test_encode
