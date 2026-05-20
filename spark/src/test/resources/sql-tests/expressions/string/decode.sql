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

-- Tests for the SQL `decode` function.
--
-- Spark's `decode` is overloaded:
--   * decode(bin, charset)                              -> StringDecode (charset binary->string)
--   * decode(expr, search, result, ..., [default])      -> CaseWhen with EqualNullSafe branches
--
-- The Oracle-style form is implemented in Spark via the RuntimeReplaceable trait, so by the
-- time Comet sees the plan the wrapper has already been replaced with CaseWhen and Comet
-- handles it through its existing CaseWhen + EqualNullSafe serde.
--
-- The 2-arg charset form lowers to a cast(binary, string) inside Comet's stringDecode
-- handler, but only when the charset is 'utf-8' (case-insensitive). Other charsets fall
-- back to Spark JVM execution.

-- ===========================================================================
-- Charset form: decode(bin, charset) for UTF-8 (the supported native path)
-- ===========================================================================

statement
CREATE TABLE test_decode_utf8(b binary) USING parquet

statement
INSERT INTO test_decode_utf8 VALUES (CAST('hello' AS BINARY)), (CAST('world' AS BINARY)), (CAST('' AS BINARY)),
    (CAST('café' AS BINARY)), (NULL)

query
SELECT decode(b, 'utf-8') FROM test_decode_utf8

query
SELECT decode(b, 'UTF-8') FROM test_decode_utf8

query
SELECT decode(CAST('hello' AS BINARY), 'utf-8'), decode(CAST('' AS BINARY), 'utf-8'), decode(NULL, 'utf-8')

-- Charset form: non-UTF-8

statement
CREATE TABLE test_decode_charset_safe(b binary) USING parquet

statement
INSERT INTO test_decode_charset_safe VALUES (CAST('ab' AS BINARY)), (CAST('abcd' AS BINARY)), (CAST('' AS BINARY)), (NULL)

query expect_fallback(Comet only supports decoding with 'utf-8'.)
SELECT decode(b, 'UTF-16BE') FROM test_decode_charset_safe

query expect_fallback(Comet only supports decoding with 'utf-8'.)
SELECT decode(b, 'US-ASCII') FROM test_decode_charset_safe

query expect_fallback(Comet only supports decoding with 'utf-8'.)
SELECT decode(b, 'ISO-8859-1') FROM test_decode_utf8


statement
CREATE TABLE test_decode_oracle(status string, code int) USING parquet

statement
INSERT INTO test_decode_oracle VALUES ('A', 1), ('I', 2), ('X', 3), (NULL, 4), ('A', NULL)

query
SELECT decode(status, 'A', 'Active', 'I', 'Inactive', 'Other') FROM test_decode_oracle

query
SELECT decode(status, 'A', 'Active', 'I', 'Inactive') FROM test_decode_oracle

query
SELECT decode(code, 1, 'one', 2, 'two', 3, 'three', 'unknown') FROM test_decode_oracle

query
SELECT decode(code, 1, 'one', 2, 'two') FROM test_decode_oracle

query
SELECT decode(status, 'A', 'has-A', NULL, 'is-null', 'other') FROM test_decode_oracle

query
SELECT decode(status, 'A', 'Active') FROM test_decode_oracle

query
SELECT decode(2, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 'Other')

query
SELECT decode(6, 1, 'Southlake', 2, 'San Francisco', 'Other')

query
SELECT decode(6, 1, 'Southlake', 2, 'San Francisco')

query
SELECT decode(NULL, 6, 'Spark', NULL, 'SQL', 4, 'rocks')
