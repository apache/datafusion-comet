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

-- AES-CBC round-trip for aes_encrypt / aes_decrypt. Comet routes the underlying StaticInvoke
-- through the JVM codegen dispatcher. AES-CBC was added to Spark in 3.5 (SPARK-43042) and
-- throws on 3.4, so this file is gated.
-- MinSparkVersion: 3.5

statement
CREATE TABLE test_aes_cbc(data STRING, key STRING) USING parquet

statement
INSERT INTO test_aes_cbc VALUES
  ('hello world', '1234567890abcdef'),
  ('apache spark', '1234567890abcdef'),
  ('', '1234567890abcdef'),
  (NULL, '1234567890abcdef')

-- CBC round-trip (nondeterministic IV, so compare via round-trip rather than raw ciphertext)
query
SELECT CAST(aes_decrypt(aes_encrypt(data, key, 'CBC'), key, 'CBC') AS STRING) FROM test_aes_cbc
