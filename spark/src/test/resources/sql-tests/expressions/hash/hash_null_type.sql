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

-- Spark's HashExpression leaves the running hash unchanged for a NULL child, so a NullType
-- argument, or a NullType struct field, is skipped. The other inputs are read from Parquet so
-- the hashes run natively.
statement
CREATE TABLE test_hash_null_type(a int, s string) USING parquet

statement
INSERT INTO test_hash_null_type VALUES (1, 'a'), (0, ''), (-1, NULL), (2147483647, 'Spark SQL'), (NULL, NULL)

query expect_native(hash, xxhash64)
SELECT a, hash(a, null), xxhash64(a, null), hash(null, a), xxhash64(null, a),
  hash(a, null, s), xxhash64(a, null, s)
FROM test_hash_null_type

query expect_native(hash, xxhash64)
SELECT hash(named_struct('x', a, 'n', null)), xxhash64(named_struct('x', a, 'n', null))
FROM test_hash_null_type

-- A nullable struct with a NullType field: the struct's nulls are pushed into the all-null field.
query expect_native(hash, xxhash64)
SELECT hash(IF(a > 0, named_struct('x', a, 'n', null), null)),
  xxhash64(IF(a > 0, named_struct('x', a, 'n', null), null))
FROM test_hash_null_type
