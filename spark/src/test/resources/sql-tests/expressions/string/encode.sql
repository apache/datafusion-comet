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

-- Tests for encode(str, charset). These cover the canonical charsets that Spark
-- supports in both legacy and strict modes, so the expected output is the same
-- across Spark 3.4, 3.5, and 4.0+. The charset-whitelist enforcement and the
-- default raise-on-unmappable behavior introduced in Spark 4.0 live in
-- encode_strict.sql.

statement
CREATE TABLE test_encode(s string, b binary) USING parquet

statement
INSERT INTO test_encode VALUES
  ('Spark SQL',   CAST(x'48656C6C6F' AS BINARY)),
  ('',            CAST(x'' AS BINARY)),
  ('naïve',       CAST(x'FFFE' AS BINARY)),
  ('😀',          CAST(x'F09F9880' AS BINARY)),
  (NULL,          NULL)

-- ============================================================================
-- UTF-8 encoding (identity for valid UTF-8 input)
-- ============================================================================

-- column argument
query spark_answer_only
SELECT hex(encode(s, 'UTF-8')) FROM test_encode

-- literal argument
query spark_answer_only
SELECT hex(encode('Spark SQL', 'UTF-8'))

-- case-insensitive charset
query spark_answer_only
SELECT hex(encode('Spark SQL', 'utf-8')), hex(encode('Spark SQL', 'Utf-8'))

-- empty string returns empty binary, not NULL
query spark_answer_only
SELECT encode('', 'UTF-8') IS NULL, length(encode('', 'UTF-8'))

-- emoji (4-byte UTF-8 sequence)
query spark_answer_only
SELECT hex(encode('😀', 'UTF-8'))

-- ============================================================================
-- US-ASCII encoding
-- ============================================================================

query spark_answer_only
SELECT hex(encode(s, 'US-ASCII')) FROM test_encode WHERE s IN ('Spark SQL', '')

query spark_answer_only
SELECT hex(encode('Hello', 'US-ASCII'))

-- ============================================================================
-- ISO-8859-1 encoding (Latin-1 characters fit in a single byte)
-- ============================================================================

query spark_answer_only
SELECT hex(encode(s, 'ISO-8859-1')) FROM test_encode WHERE s IN ('Spark SQL', 'naïve')

query spark_answer_only
SELECT hex(encode('naïve', 'ISO-8859-1'))

-- ============================================================================
-- UTF-16 encoding (Spark emits a big-endian BOM FEFF followed by UTF-16BE)
-- ============================================================================

query spark_answer_only
SELECT hex(encode('AB', 'UTF-16'))

-- emoji encodes as a surrogate pair, still preceded by the BOM
query spark_answer_only
SELECT hex(encode('😀', 'UTF-16'))

-- ============================================================================
-- UTF-16BE encoding (no BOM)
-- ============================================================================

query spark_answer_only
SELECT hex(encode('AB', 'UTF-16BE'))

-- emoji surrogate pair, big-endian
query spark_answer_only
SELECT hex(encode('😀', 'UTF-16BE'))

-- ============================================================================
-- UTF-16LE encoding (no BOM)
-- ============================================================================

query spark_answer_only
SELECT hex(encode('AB', 'UTF-16LE'))

-- emoji surrogate pair, little-endian
query spark_answer_only
SELECT hex(encode('😀', 'UTF-16LE'))

-- ============================================================================
-- UTF-32 encoding (Spark does NOT emit a BOM for UTF-32)
-- ============================================================================

query spark_answer_only
SELECT hex(encode('A', 'UTF-32'))

query spark_answer_only
SELECT hex(encode('😀', 'UTF-32'))

-- ============================================================================
-- NULL handling
-- ============================================================================

-- NULL string input returns NULL
query spark_answer_only
SELECT hex(encode(CAST(NULL AS STRING), 'UTF-8'))

-- NULL charset returns NULL
query spark_answer_only
SELECT hex(encode('hello', CAST(NULL AS STRING)))

-- NULL in a column
query spark_answer_only
SELECT hex(encode(s, 'UTF-8')) FROM test_encode WHERE s IS NULL

-- ============================================================================
-- Binary input (Spark implicitly casts BINARY to STRING, invalid UTF-8 bytes
-- become U+FFFD which is EF BF BD in UTF-8)
-- ============================================================================

-- valid UTF-8 binary round-trips
query spark_answer_only
SELECT hex(encode(CAST(x'48656C6C6F' AS BINARY), 'UTF-8'))

-- invalid UTF-8 binary: each invalid byte becomes U+FFFD
query spark_answer_only
SELECT hex(encode(CAST(x'FFFE' AS BINARY), 'UTF-8'))

-- binary column input
query spark_answer_only
SELECT hex(encode(b, 'UTF-8')) FROM test_encode WHERE b IS NOT NULL
