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

-- Tests for the strict charset whitelist and raise-on-unmappable behavior that
-- Spark 4.0 enabled by default. Earlier Spark versions have
-- spark.sql.legacy.javaCharsets=true and spark.sql.legacy.codingErrorAction=true
-- by default, which permit extra aliases and replace unmappable characters with
-- '?', so these assertions only hold on Spark 4.0 and later.

-- MinSparkVersion: 4.0

-- ============================================================================
-- Charset whitelist: Spark accepts exactly us-ascii, iso-8859-1, utf-8,
-- utf-16, utf-16be, utf-16le, utf-32. Anything else raises
-- INVALID_PARAMETER_VALUE.CHARSET.
-- ============================================================================

-- UTF-32BE and UTF-32LE are not accepted (only UTF-32 is)
query expect_error(INVALID_PARAMETER_VALUE.CHARSET)
SELECT encode('A', 'UTF-32BE')

query expect_error(INVALID_PARAMETER_VALUE.CHARSET)
SELECT encode('A', 'UTF-32LE')

-- Aliases without the hyphen are not accepted
query expect_error(INVALID_PARAMETER_VALUE.CHARSET)
SELECT encode('abc', 'UTF8')

query expect_error(INVALID_PARAMETER_VALUE.CHARSET)
SELECT encode('abc', 'UTF16')

query expect_error(INVALID_PARAMETER_VALUE.CHARSET)
SELECT encode('abc', 'UTF16BE')

-- ASCII without the US- prefix is not accepted
query expect_error(INVALID_PARAMETER_VALUE.CHARSET)
SELECT encode('abc', 'ASCII')

-- ISO-8859-1 aliases LATIN1 and ISO88591 are not accepted
query expect_error(INVALID_PARAMETER_VALUE.CHARSET)
SELECT encode('abc', 'LATIN1')

query expect_error(INVALID_PARAMETER_VALUE.CHARSET)
SELECT encode('abc', 'ISO88591')

-- Completely unknown charsets
query expect_error(INVALID_PARAMETER_VALUE.CHARSET)
SELECT encode('abc', 'EBCDIC')

-- ============================================================================
-- Raise on unmappable characters (legacy.codingErrorAction defaults to false)
-- ============================================================================

-- U+00E9 (é) is not representable in US-ASCII
query expect_error(MALFORMED_CHARACTER_CODING)
SELECT encode('é', 'US-ASCII')

-- U+0100 (Ā) is not representable in ISO-8859-1
query expect_error(MALFORMED_CHARACTER_CODING)
SELECT encode(CAST(x'C480' AS BINARY), 'ISO-8859-1')

-- emoji is not representable in US-ASCII
query expect_error(MALFORMED_CHARACTER_CODING)
SELECT encode('😀', 'US-ASCII')

-- column argument with an unmappable value also raises
statement
CREATE TABLE test_encode_unmappable(s string) USING parquet

statement
INSERT INTO test_encode_unmappable VALUES ('é')

query expect_error(MALFORMED_CHARACTER_CODING)
SELECT encode(s, 'US-ASCII') FROM test_encode_unmappable
