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

-- ConfigMatrix: spark.comet.exec.scalaUDF.codegen.enabled=true,false
-- ConstantFolding: enabled

-- Keep payloads in Parquet columns so SHA-2 executes even when numBits is folded.
statement
CREATE TABLE test_sha2_string(payload STRING, num_bits INT) USING parquet

statement
INSERT INTO test_sha2_string VALUES
  ('hello', 0), ('hello', 224), ('hello', 256), ('hello', 384), ('hello', 512),
  ('hello', 128), ('hello', -1), ('hello', NULL), ('', 256), ('', 512), (NULL, 256)

-- Row-varying bit widths, including invalid widths and NULL, must stay native.
query expect_native(sha2)
SELECT sha2(payload, num_bits) FROM test_sha2_string

query expect_native(sha2)
SELECT sha2(payload, num_bits + 0) FROM test_sha2_string

query expect_native(sha2)
SELECT sha2('hello', num_bits) FROM test_sha2_string

-- Literal widths and a foldable width expression exercise the existing native path.
query expect_native(sha2)
SELECT sha2(payload, 0), sha2(payload, 224), sha2(payload, 256),
  sha2(payload, 384), sha2(payload, 512), sha2(payload, 128),
  sha2(payload, -1), sha2(payload, 128 + 128)
FROM test_sha2_string

-- Include zero and high bytes to exercise binary payloads without string conversion.
statement
CREATE TABLE test_sha2_binary(payload BINARY, num_bits INT) USING parquet

statement
INSERT INTO test_sha2_binary VALUES
  (X'0080FF68656C6C6F', 0), (X'0080FF68656C6C6F', 224),
  (X'0080FF68656C6C6F', 256), (X'0080FF68656C6C6F', 384),
  (X'0080FF68656C6C6F', 512), (X'0080FF68656C6C6F', 128),
  (X'0080FF68656C6C6F', -1), (X'0080FF68656C6C6F', NULL),
  (X'', 256), (X'', 512), (NULL, 256)

query expect_native(sha2)
SELECT sha2(payload, num_bits) FROM test_sha2_binary

query expect_native(sha2)
SELECT sha2(payload, num_bits + 0) FROM test_sha2_binary

query expect_native(sha2)
SELECT sha2(X'0080FF68656C6C6F', num_bits) FROM test_sha2_binary
