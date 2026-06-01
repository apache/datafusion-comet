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

-- Tests for aes_encrypt, aes_decrypt, try_aes_decrypt
-- All three functions fall back to Spark because StaticInvoke (aesEncrypt/aesDecrypt)
-- is not yet supported as a Comet native expression.

statement
CREATE TABLE test_aes(data STRING, key STRING) USING parquet

statement
INSERT INTO test_aes VALUES
  ('hello world', '1234567890abcdef'),
  ('apache spark', '1234567890abcdef'),
  ('', '1234567890abcdef'),
  (NULL, '1234567890abcdef')

-- GCM round-trip (default mode, nondeterministic IV, test via round-trip)
query spark_answer_only
SELECT CAST(aes_decrypt(aes_encrypt(data, key), key) AS STRING) FROM test_aes

-- GCM round-trip with explicit mode
query spark_answer_only
SELECT CAST(aes_decrypt(aes_encrypt(data, key, 'GCM'), key, 'GCM') AS STRING) FROM test_aes

-- ECB round-trip (deterministic mode)
query spark_answer_only
SELECT CAST(aes_decrypt(aes_encrypt(data, key, 'ECB'), key, 'ECB') AS STRING) FROM test_aes

-- CBC round-trip (nondeterministic IV)
query spark_answer_only
SELECT CAST(aes_decrypt(aes_encrypt(data, key, 'CBC'), key, 'CBC') AS STRING) FROM test_aes

-- ECB direct: output is deterministic so we can compare directly to Spark
query spark_answer_only
SELECT aes_encrypt(data, key, 'ECB') FROM test_aes

-- aes_decrypt on ECB-encrypted column
query spark_answer_only
SELECT CAST(aes_decrypt(aes_encrypt(data, key, 'ECB'), key, 'ECB') AS STRING) FROM test_aes

-- literal key and data (all literals, constant folding disabled in test suite)
query spark_answer_only
SELECT CAST(aes_decrypt(aes_encrypt('hello', '1234567890abcdef', 'ECB'), '1234567890abcdef', 'ECB') AS STRING)

query spark_answer_only
SELECT CAST(aes_decrypt(aes_encrypt(NULL, '1234567890abcdef', 'ECB'), '1234567890abcdef', 'ECB') AS STRING)

-- 24-byte key
query spark_answer_only
SELECT CAST(aes_decrypt(aes_encrypt(data, '1234567890abcdef12345678', 'ECB'), '1234567890abcdef12345678', 'ECB') AS STRING) FROM test_aes

-- 32-byte key
query spark_answer_only
SELECT CAST(aes_decrypt(aes_encrypt(data, '1234567890abcdef1234567890abcdef', 'ECB'), '1234567890abcdef1234567890abcdef', 'ECB') AS STRING) FROM test_aes

-- try_aes_decrypt: invalid ciphertext returns NULL instead of throwing
query spark_answer_only
SELECT try_aes_decrypt(CAST('garbage' AS BINARY), key) FROM test_aes

-- try_aes_decrypt: valid ciphertext decrypts correctly
query spark_answer_only
SELECT CAST(try_aes_decrypt(aes_encrypt(data, key, 'ECB'), key, 'ECB') AS STRING) FROM test_aes

-- try_aes_decrypt with literal invalid ciphertext
query spark_answer_only
SELECT try_aes_decrypt(CAST('not_valid_ciphertext' AS BINARY), '1234567890abcdef')

-- try_aes_decrypt: NULL data
query spark_answer_only
SELECT try_aes_decrypt(NULL, '1234567890abcdef')
