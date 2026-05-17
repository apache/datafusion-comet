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

-- Tests for the SQL `encode(str, charset)` function.
--
-- Spark 3.x: Encode is a BinaryExpression(value, charset).
-- Spark 4.x+: Encode is RuntimeReplaceable; the analyzer rewrites it to
--   StaticInvoke(classOf[Encode], BinaryType, "encode", ...)

statement
CREATE TABLE test_encode_utf8(s string) USING parquet

statement
INSERT INTO test_encode_utf8 VALUES ('hello'), ('world'), (''), ('café'), (NULL)

query
SELECT encode(s, 'utf-8') FROM test_encode_utf8

query
SELECT encode(s, 'UTF-8') FROM test_encode_utf8

-- Mixed-case charset literal exercises toLowerCase normalization
query
SELECT encode(s, 'Utf-8') FROM test_encode_utf8

query
SELECT encode('hello', 'utf-8'), encode('', 'utf-8'), encode(CAST(NULL AS STRING), 'utf-8')

-- Different language(French, Japanese)
query
SELECT encode('café', 'utf-8'), encode('日本語', 'utf-8')

-- non-UTF-8 falls back to Spark JVM
statement
CREATE TABLE test_encode_charset_safe(s string) USING parquet

statement
INSERT INTO test_encode_charset_safe VALUES ('hello'), ('world'), (''), (NULL)

query expect_fallback(Comet only supports encoding with 'utf-8'.)
SELECT encode(s, 'UTF-16BE') FROM test_encode_charset_safe

query expect_fallback(Comet only supports encoding with 'utf-8'.)
SELECT encode(s, 'US-ASCII') FROM test_encode_charset_safe

query expect_fallback(Comet only supports encoding with 'utf-8'.)
SELECT encode(s, 'ISO-8859-1') FROM test_encode_charset_safe