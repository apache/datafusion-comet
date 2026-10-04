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

-- encode() under `spark.sql.legacy.javaCharsets=true` on Spark 4.0+.
--
-- The flag is baked into the `Encode` node at analysis time and carried through to
-- `StaticInvoke(classOf[Encode], "encode", ...)` as a literal, so it reaches the codegen
-- dispatcher and `CharsetProvider.forName` accepts any JVM charset again. Without the flag the
-- same queries raise `INVALID_PARAMETER_VALUE.CHARSET`; see encode_invalid_charset_strict.sql.
-- MinSparkVersion: 4.0
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.legacy.javaCharsets=true

statement
CREATE TABLE test_encode_legacy_charsets(s string) USING parquet

-- All rows are representable in windows-1252 ('é' is 0xE9), so the strict coding-error action
-- that stays in effect here does not fire.
statement
INSERT INTO test_encode_legacy_charsets VALUES ('hello'), ('café'), (''), (NULL)

query
SELECT encode(s, 'windows-1252') FROM test_encode_legacy_charsets

query
SELECT encode('café', 'windows-1252'), encode('hello', 'Shift_JIS')

-- The allowlisted charsets keep working with the flag on.

query
SELECT encode(s, 'UTF-16LE') FROM test_encode_legacy_charsets
