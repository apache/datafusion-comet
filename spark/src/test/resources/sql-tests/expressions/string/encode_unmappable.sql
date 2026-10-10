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

-- encode() over characters the target charset cannot represent, on Spark 3.4 and 3.5.
--
-- On those versions `Encode` is a plain BinaryExpression whose eval is
-- `input.toString.getBytes(charset)`, and `String.getBytes` uses an encoder configured with
-- `CodingErrorAction.REPLACE`. Unmappable characters therefore become the charset's replacement
-- byte (0x3F, '?') rather than raising. Spark 4.0+ builds the encoder with
-- `CodingErrorAction.REPORT` and throws instead; that branch is covered by the paired fixture
-- encode_unmappable_strict.sql.
-- MaxSparkVersion: 3.5
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_encode_unmappable(s string) USING parquet

statement
INSERT INTO test_encode_unmappable VALUES ('café'), ('中文'), ('hello'), (''), (NULL)

-- Column input: 'é' and the CJK characters are unmappable in US-ASCII and are substituted.

query
SELECT encode(s, 'US-ASCII') FROM test_encode_unmappable

-- 'é' is representable in ISO-8859-1 (0xE9) but the CJK characters are not.

query
SELECT encode(s, 'ISO-8859-1') FROM test_encode_unmappable

-- Literal input, pinning the substituted bytes rather than only asserting Spark/Comet agreement.

query
SELECT encode('café', 'US-ASCII') = CAST('caf?' AS BINARY),
       encode('中文', 'US-ASCII') = CAST('??' AS BINARY),
       encode('中文', 'ISO-8859-1') = CAST('??' AS BINARY)
