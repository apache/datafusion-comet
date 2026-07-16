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

-- Tests for try_aes_decrypt, which returns NULL on invalid input instead of throwing.
-- try_aes_decrypt lowers to TryEval(StaticInvoke(aesDecrypt, ...)); Comet routes both the
-- TryEval wrapper and the inner StaticInvoke through the JVM codegen dispatcher.
-- try_aes_decrypt was added in Spark 3.5, so this file is gated.
-- MinSparkVersion: 3.5

statement
CREATE TABLE test_aes_try(data STRING, key STRING) USING parquet

statement
INSERT INTO test_aes_try VALUES
  ('hello world', '1234567890abcdef'),
  ('apache spark', '1234567890abcdef'),
  ('', '1234567890abcdef'),
  (NULL, '1234567890abcdef')

-- invalid ciphertext returns NULL instead of throwing
query
SELECT try_aes_decrypt(CAST('garbage' AS BINARY), key) FROM test_aes_try

-- valid ciphertext decrypts correctly
query
SELECT CAST(try_aes_decrypt(aes_encrypt(data, key, 'ECB'), key, 'ECB') AS STRING) FROM test_aes_try

-- literal invalid ciphertext
query
SELECT try_aes_decrypt(CAST('not_valid_ciphertext' AS BINARY), '1234567890abcdef')

-- NULL data
query
SELECT try_aes_decrypt(NULL, '1234567890abcdef')
