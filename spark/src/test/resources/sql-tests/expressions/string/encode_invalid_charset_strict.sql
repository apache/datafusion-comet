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

-- encode() charset validation in Spark 4.0's default mode.
--
-- `CharsetProvider.forName` permits only us-ascii, iso-8859-1, utf-8, utf-16be, utf-16le, utf-16
-- and utf-32 unless `spark.sql.legacy.javaCharsets` is enabled, and otherwise raises
-- `INVALID_PARAMETER_VALUE.CHARSET`. On Spark 3.4/3.5 a JVM charset outside that list is simply
-- accepted; see encode_invalid_charset.sql. Enabling the legacy flag restores that behavior on
-- 4.0+; see encode_legacy_charsets.sql.
-- MinSparkVersion: 4.0
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_encode_charset_strict(s string) USING parquet

statement
INSERT INTO test_encode_charset_strict VALUES ('hello'), ('café'), (''), (NULL)

-- Sentinel: an allowlisted charset, asserting `encode` executes natively so the expect_error
-- queries below trip the kernel rather than being satisfied by an operator-level Spark fallback.

query
SELECT encode(s, 'utf-8') FROM test_encode_charset_strict

-- windows-1252 is a valid JVM charset but is outside VALID_CHARSETS.
query expect_error(INVALID_PARAMETER_VALUE)
SELECT encode('hello', 'windows-1252')

-- Column input takes the same path.
query expect_error(INVALID_PARAMETER_VALUE)
SELECT encode(s, 'windows-1252') FROM test_encode_charset_strict

-- A charset name the JVM does not know at all is rejected by the same check.
query expect_error(INVALID_PARAMETER_VALUE)
SELECT encode('hello', 'NO-SUCH-CHARSET')
