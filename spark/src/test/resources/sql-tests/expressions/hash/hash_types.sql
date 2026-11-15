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

-- Read typed columns from Parquet so hashing cannot be evaluated by the optimizer.
statement
CREATE TABLE test_hash_boolean(c BOOLEAN) USING parquet

statement
INSERT INTO test_hash_boolean VALUES (true), (false), (NULL)

query expect_native(hash, xxhash64)
SELECT c, hash(c), xxhash64(c) FROM test_hash_boolean

statement
CREATE TABLE test_hash_tinyint(c TINYINT) USING parquet

statement
INSERT INTO test_hash_tinyint VALUES (1), (0), (-1), (127), (-128), (NULL)

query expect_native(hash, xxhash64)
SELECT c, hash(c), xxhash64(c) FROM test_hash_tinyint

statement
CREATE TABLE test_hash_smallint(c SMALLINT) USING parquet

statement
INSERT INTO test_hash_smallint VALUES (1), (0), (-1), (32767), (-32768), (NULL)

query expect_native(hash, xxhash64)
SELECT c, hash(c), xxhash64(c) FROM test_hash_smallint

statement
CREATE TABLE test_hash_int(c INT) USING parquet

statement
INSERT INTO test_hash_int VALUES (1), (0), (-1), (2147483647), (-2147483648), (NULL)

query expect_native(hash, xxhash64)
SELECT c, hash(c), xxhash64(c) FROM test_hash_int

statement
CREATE TABLE test_hash_bigint(c BIGINT) USING parquet

statement
INSERT INTO test_hash_bigint VALUES
  (1), (0), (-1), (9223372036854775807), (-9223372036854775808), (NULL)

query expect_native(hash, xxhash64)
SELECT c, hash(c), xxhash64(c) FROM test_hash_bigint

statement
CREATE TABLE test_hash_binary(c BINARY) USING parquet

statement
INSERT INTO test_hash_binary VALUES (X''), (X'00'), (X'0102030405'), (NULL)

query expect_native(hash, xxhash64)
SELECT c, hash(c), xxhash64(c) FROM test_hash_binary

statement
CREATE TABLE test_hash_date(c DATE) USING parquet

statement
INSERT INTO test_hash_date VALUES
  (DATE '2023-01-01'), (DATE '1970-01-01'), (DATE '2000-12-31'),
  (DATE '1969-12-31'), (NULL)

query expect_native(hash, xxhash64)
SELECT c, hash(c), xxhash64(c) FROM test_hash_date

statement
CREATE TABLE test_hash_timestamp(c TIMESTAMP, ntz TIMESTAMP_NTZ) USING parquet

statement
INSERT INTO test_hash_timestamp VALUES
  (TIMESTAMP '2023-01-01 12:00:00', TIMESTAMP_NTZ '2023-01-01 12:00:00'),
  (TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP_NTZ '1970-01-01 00:00:00'),
  (TIMESTAMP '2000-12-31 23:59:59', TIMESTAMP_NTZ '2000-12-31 23:59:59'),
  (TIMESTAMP '1969-12-31 23:59:59.999999', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999'),
  (NULL, NULL)

query expect_native(hash, xxhash64)
SELECT c, hash(c), xxhash64(c), ntz, hash(ntz), xxhash64(ntz) FROM test_hash_timestamp
