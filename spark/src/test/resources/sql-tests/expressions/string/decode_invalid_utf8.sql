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

-- decode() over invalid UTF-8 byte sequences with legacy / replacement-character semantics.
--
-- On Spark 3.4 and 3.5 `decode(bin, charset)` always substitutes the Unicode replacement
-- character for malformed sequences (it lowers to `new String(bytes, charset)`, which uses the
-- JVM's default replace-on-error decoder).
-- On Spark 4.0+ the same substitute behavior is selected by enabling both
-- `spark.sql.legacy.javaCharsets` and `spark.sql.legacy.codingErrorAction`.
-- The 4.0 default (strict) mode is covered separately in decode_invalid_utf8_strict.sql.
--
-- Regression coverage for #4465: prior to that fix Comet lowered `decode` to a TRY-mode binary→
-- string cast, which produced wrong output (NULL or raw bytes) on invalid sequences regardless of
-- mode. The codegen dispatcher path delegates to Spark's own decoder so this fixture verifies the
-- replacement-character output matches Spark.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.legacy.javaCharsets=true
-- Config: spark.sql.legacy.codingErrorAction=true

statement
CREATE TABLE test_decode_invalid_utf8(b binary) USING parquet

-- 0xFF: a continuation-byte-only sequence that is not valid UTF-8.
-- 0xC3 0x28: a 2-byte sequence whose continuation byte (0x28) is invalid.
-- 0xE2 0x82 0x28: a 3-byte sequence with an invalid continuation byte.
-- 'caf' || 0xE9: ISO-8859-1 'café' bytes — 0xE9 is invalid as a UTF-8 lead byte.
statement
INSERT INTO test_decode_invalid_utf8 VALUES
    (X'FF'),
    (X'C328'),
    (X'E28228'),
    (CONCAT(CAST('caf' AS BINARY), X'E9')),
    (CAST('valid' AS BINARY)),
    (NULL)

query
SELECT decode(b, 'utf-8') FROM test_decode_invalid_utf8

query
SELECT decode(b, 'UTF-8') FROM test_decode_invalid_utf8

query
SELECT decode(X'FF', 'utf-8'),
       decode(X'C328', 'utf-8'),
       decode(X'E28228', 'utf-8')
