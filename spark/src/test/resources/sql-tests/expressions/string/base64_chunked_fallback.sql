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

-- When spark.sql.chunkBase64String.enabled=true (the Spark default) and the user has not
-- opted in to incompatible expressions, Comet must defer to Spark instead of silently
-- producing un-chunked output.
-- Config: spark.sql.chunkBase64String.enabled=true

-- Spark 3.4 has no chunkBase64 parameter (always chunks), so this test is only meaningful
-- on 3.5 or newer where the conf governs behavior.
-- MinSparkVersion: 3.5

statement
CREATE TABLE test_base64_chunked(b binary) USING parquet

statement
INSERT INTO test_base64_chunked VALUES
  (CAST('Spark SQL' AS binary)),
  (NULL)

query expect_fallback(not fully compatible with Spark)
SELECT base64(b) FROM test_base64_chunked
