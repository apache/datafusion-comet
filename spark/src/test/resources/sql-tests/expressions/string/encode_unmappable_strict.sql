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

-- encode() over characters the target charset cannot represent, in Spark 4.0's default
-- (strict) mode.
--
-- Spark 4.0 added `spark.sql.legacy.codingErrorAction` (default `false`), which builds the
-- encoder with `CodingErrorAction.REPORT`. `Encode.encode` converts the resulting
-- `CharacterCodingException` into `MALFORMED_CHARACTER_CODING`. On Spark 3.4/3.5 the same input
-- is silently substituted; that branch is covered by the paired fixture encode_unmappable.sql.
-- MinSparkVersion: 4.0
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_encode_unmappable_strict(s string) USING parquet

statement
INSERT INTO test_encode_unmappable_strict VALUES ('café'), ('中文'), (NULL)

-- Sentinel: ensures Comet actually runs `encode` (codegen dispatcher) so the expect_error queries
-- below trip the kernel rather than being satisfied by an operator-level Spark fallback.
statement
CREATE TABLE test_encode_strict_sentinel(s string) USING parquet

statement
INSERT INTO test_encode_strict_sentinel VALUES ('hello'), (''), (NULL)

query
SELECT encode(s, 'US-ASCII') FROM test_encode_strict_sentinel

-- 'é' has no US-ASCII representation.
query expect_error(MALFORMED_CHARACTER_CODING)
SELECT encode('café', 'US-ASCII')

-- CJK characters have no ISO-8859-1 representation.
query expect_error(MALFORMED_CHARACTER_CODING)
SELECT encode('中文', 'ISO-8859-1')

-- Column input takes the same path.
query expect_error(MALFORMED_CHARACTER_CODING)
SELECT encode(s, 'US-ASCII') FROM test_encode_unmappable_strict
