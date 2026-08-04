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

-- decode() over invalid UTF-8 byte sequences in Spark 4.0's default (strict) mode.
--
-- Spark 4.0 added `spark.sql.legacy.codingErrorAction` (default `false`) which replaces the JVM
-- default substitute-on-error decoder with one that throws `MALFORMED_CHARACTER_CODING`. This
-- fixture asserts both Spark and Comet raise that error, with a sentinel valid-input query so the
-- assertion does not pass vacuously through an operator-level fallback.
-- Regression coverage for #4465.
-- MinSparkVersion: 4.0
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

-- Sentinel: ensures Comet actually runs `decode` (codegen dispatcher) so the expect_error queries
-- below trip the kernel rather than being satisfied by an operator-level Spark fallback.
statement
CREATE TABLE test_decode_strict_sentinel(b binary) USING parquet

statement
INSERT INTO test_decode_strict_sentinel VALUES (CAST('hello' AS BINARY)), (NULL)

query
SELECT decode(b, 'utf-8') FROM test_decode_strict_sentinel

-- 0xFF is not a valid UTF-8 lead byte; strict mode raises.
query expect_error(MALFORMED_CHARACTER_CODING)
SELECT decode(X'FF', 'utf-8')

-- 0xC3 0x28: 2-byte sequence with invalid continuation.
query expect_error(MALFORMED_CHARACTER_CODING)
SELECT decode(X'C328', 'utf-8')

-- 0xE2 0x82 0x28: 3-byte sequence with invalid continuation.
query expect_error(MALFORMED_CHARACTER_CODING)
SELECT decode(X'E28228', 'utf-8')
