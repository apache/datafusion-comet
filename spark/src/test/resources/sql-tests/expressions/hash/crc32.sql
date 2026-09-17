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

-- crc32 function
statement
CREATE TABLE test(col string, a int, b float) USING parquet

statement
INSERT INTO test VALUES ('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999), ('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999)

query
SELECT crc32(col), crc32(cast(a as string)), crc32(cast(b as string)) FROM test

-- literal arguments
query
SELECT crc32('Spark SQL')

-- Binary inputs must be hashed as bytes, including invalid UTF-8 and embedded NULs.
statement
CREATE TABLE test_crc32_binary(b BINARY) USING parquet

statement
INSERT INTO test_crc32_binary VALUES
  (X'00FF80'), (X'610062'), (X'E88BB9E69E9C'),
  (X'313233343536373839'), (X''), (NULL)

query
SELECT crc32(b) FROM test_crc32_binary

-- The checksum of '123456789' exceeds the signed 32-bit range.
query
SELECT crc32(X'313233343536373839'), crc32(X'00FF80'), crc32(X'610062'),
  crc32(X''), crc32(CAST(NULL AS BINARY))
