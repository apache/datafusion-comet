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

-- Comet's base64 always produces unchunked padded output. Spark's default chunks output
-- every 76 characters, so we run with chunkBase64String.enabled=false to match. Spark 3.4
-- always chunks regardless of the conf, so allowIncompatible=true is needed there.
-- Config: spark.sql.chunkBase64String.enabled=false
-- Config: spark.comet.expr.allowIncompatible=true

statement
CREATE TABLE test_base64(b binary, s string) USING parquet

statement
INSERT INTO test_base64 VALUES
  (CAST('Spark SQL' AS binary), 'Spark SQL'),
  (CAST('' AS binary), ''),
  (CAST('a' AS binary), 'a'),
  (CAST('ab' AS binary), 'ab'),
  (CAST('abc' AS binary), 'abc'),
  (CAST('hello world' AS binary), 'hello world'),
  -- 58 bytes: encodes to 80 base64 chars, the SPARK-47307 chunking boundary
  (CAST('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AS binary),
   'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
  -- 200 bytes: encodes to 268 base64 chars, deep into the chunked-vs-unchunked zone
  (CAST(repeat('xy', 100) AS binary), repeat('xy', 100)),
  -- full 0x00..0xFF byte range exercises every byte value
  (X'000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F606162636465666768696A6B6C6D6E6F707172737475767778797A7B7C7D7E7F808182838485868788898A8B8C8D8E8F909192939495969798999A9B9C9D9E9FA0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBFC0C1C2C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDFE0E1E2E3E4E5E6E7E8E9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF',
   NULL),
  (NULL, NULL)

-- column argument (binary)
query
SELECT base64(b) FROM test_base64

-- column argument (string is implicitly cast to binary)
query
SELECT base64(s) FROM test_base64

-- literal arguments
query
SELECT base64(CAST('Spark SQL' AS binary)),
       base64(CAST('' AS binary)),
       base64(CAST(NULL AS binary)),
       base64('a'),
       base64('ab'),
       base64('abc')

-- 58-byte literal: SPARK-47307 boundary, must produce 80-char unchunked output
query
SELECT base64(CAST('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AS binary))
