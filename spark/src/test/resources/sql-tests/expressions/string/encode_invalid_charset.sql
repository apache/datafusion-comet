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

-- encode() charset validation on Spark 3.4 and 3.5.
--
-- On those versions `Encode` calls `String.getBytes(charset)` directly, so any charset the JVM
-- knows is accepted (including ones outside Spark 4.0's allowlist, e.g. windows-1252) and an
-- unknown name surfaces as `UnsupportedEncodingException` through `Platform.throwException`.
-- Spark 4.0+ instead routes through `CharsetProvider.forName`, which rejects anything outside
-- VALID_CHARSETS unless `spark.sql.legacy.javaCharsets` is set; those two branches are covered by
-- encode_invalid_charset_strict.sql and encode_legacy_charsets.sql.
-- MaxSparkVersion: 3.5
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_encode_charset_legacy(s string) USING parquet

statement
INSERT INTO test_encode_charset_legacy VALUES ('hello'), ('café'), (''), (NULL)

-- windows-1252 is not in Spark 4.0's VALID_CHARSETS but is a JVM charset, so 3.x accepts it.
-- This is also the sentinel query: it fails if `encode` did not execute natively, so the
-- expect_error query below cannot pass vacuously through an operator-level fallback.

query
SELECT encode(s, 'windows-1252') FROM test_encode_charset_legacy

-- A charset name the JVM does not know at all.
query expect_error(UnsupportedEncodingException)
SELECT encode('hello', 'NO-SUCH-CHARSET')
