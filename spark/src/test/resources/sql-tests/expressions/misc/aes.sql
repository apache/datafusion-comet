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

-- Tests for aes_encrypt and aes_decrypt (available since Spark 3.3). They lower to a
-- StaticInvoke of ExpressionImplUtils.aesEncrypt / aesDecrypt; Comet routes both methods
-- through the JVM codegen dispatcher (no native lowering).
-- try_aes_decrypt (Spark 3.5+) is covered in aes_try_decrypt.sql and AES-CBC (Spark 3.5+) in
-- aes_cbc.sql, both gated with MinSparkVersion.

statement
CREATE TABLE test_aes(data STRING, key STRING) USING parquet

statement
INSERT INTO test_aes VALUES
  ('hello world', '1234567890abcdef'),
  ('apache spark', '1234567890abcdef'),
  ('', '1234567890abcdef'),
  (NULL, '1234567890abcdef')

-- GCM round-trip (default mode, nondeterministic IV, test via round-trip)
query
SELECT CAST(aes_decrypt(aes_encrypt(data, key), key) AS STRING) FROM test_aes

-- GCM round-trip with explicit mode
query
SELECT CAST(aes_decrypt(aes_encrypt(data, key, 'GCM'), key, 'GCM') AS STRING) FROM test_aes

-- ECB round-trip (deterministic mode)
query
SELECT CAST(aes_decrypt(aes_encrypt(data, key, 'ECB'), key, 'ECB') AS STRING) FROM test_aes

-- CBC mode is covered separately in aes_cbc.sql (Spark added AES-CBC in 3.5).

-- ECB direct: output is deterministic so we can compare directly to Spark
query
SELECT aes_encrypt(data, key, 'ECB') FROM test_aes

-- aes_decrypt on ECB-encrypted column
query
SELECT CAST(aes_decrypt(aes_encrypt(data, key, 'ECB'), key, 'ECB') AS STRING) FROM test_aes

-- literal key and data (all literals, constant folding disabled in test suite)
query
SELECT CAST(aes_decrypt(aes_encrypt('hello', '1234567890abcdef', 'ECB'), '1234567890abcdef', 'ECB') AS STRING)

query
SELECT CAST(aes_decrypt(aes_encrypt(NULL, '1234567890abcdef', 'ECB'), '1234567890abcdef', 'ECB') AS STRING)

-- 24-byte key
query
SELECT CAST(aes_decrypt(aes_encrypt(data, '1234567890abcdef12345678', 'ECB'), '1234567890abcdef12345678', 'ECB') AS STRING) FROM test_aes

-- 32-byte key
query
SELECT CAST(aes_decrypt(aes_encrypt(data, '1234567890abcdef1234567890abcdef', 'ECB'), '1234567890abcdef1234567890abcdef', 'ECB') AS STRING) FROM test_aes
