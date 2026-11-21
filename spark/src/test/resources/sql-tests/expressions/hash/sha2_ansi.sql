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

-- Config: spark.sql.ansi.enabled=true

-- NULL payloads must skip the ANSI cast of bits, even when the string is invalid.
-- Mixed column values keep the short-circuit check in runtime evaluation.
statement
CREATE TABLE test_sha2_ansi_string(payload STRING, bits STRING) USING parquet

statement
INSERT INTO test_sha2_ansi_string VALUES
  (NULL, 'invalid'), ('hello', '256'), ('hello', NULL), (NULL, '256')

query expect_native(sha2)
SELECT sha2(payload, CAST(bits AS INT)) FROM test_sha2_ansi_string

statement
CREATE TABLE test_sha2_ansi_binary(payload BINARY, bits STRING) USING parquet

statement
INSERT INTO test_sha2_ansi_binary VALUES
  (NULL, 'invalid'), (X'0080FF', '256'), (X'0080FF', NULL), (NULL, '256')

query expect_native(sha2)
SELECT sha2(payload, CAST(bits AS INT)) FROM test_sha2_ansi_binary
