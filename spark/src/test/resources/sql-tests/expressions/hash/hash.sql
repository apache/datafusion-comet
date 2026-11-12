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

-- Config: spark.sql.legacy.allowHashOnMapType=true
-- ConfigMatrix: parquet.enable.dictionary=false,true

-- hash functions
statement
CREATE TABLE test(col string, a int, b float) USING parquet

statement
INSERT INTO test VALUES ('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999), ('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999)

query
SELECT md5(col), md5(cast(a as string)), md5(cast(b as string)), hash(col), hash(col, 1), hash(col, 0), hash(col, a, b), hash(b, a, col), xxhash64(col), xxhash64(col, 1), xxhash64(col, 0), xxhash64(col, a, b), xxhash64(b, a, col), sha2(col, 0), sha2(col, 256), sha2(col, 224), sha2(col, 384), sha2(col, 512), sha2(col, 128), sha2(col, -1), sha1(col), sha1(cast(a as string)), sha1(cast(b as string)) FROM test

-- Single-column strings, including the repeated inputs from the dictionary and md5 tests.
statement
CREATE TABLE test_hash_strings(c STRING) USING parquet

statement
INSERT INTO test_hash_strings VALUES
  ('hello'), (''), ('Spark SQL'), ('苹果手机'), (NULL),
  ('a'), ('b'), ('a'), ('b'), ('a'), ('c'),
  ('test1'), ('test1'), ('test2'), ('test2')

query expect_native(hash, xxhash64, md5)
SELECT c, hash(c), xxhash64(c), md5(c) FROM test_hash_strings

-- Keep DOUBLE distinct from the FLOAT column above and exercise both child orders.
statement
CREATE TABLE test_hash_columns(a INT, b STRING, c DOUBLE) USING parquet

statement
INSERT INTO test_hash_columns VALUES
  (1, 'hello', 3.14), (2, '', 0.0), (NULL, NULL, NULL), (-1, 'test', -1.5), (3, 'test', 1.0)

query expect_native(hash, xxhash64)
SELECT hash(a, b, c), xxhash64(a, b, c), hash(c, b, a), xxhash64(c, b, a),
  hash(a), xxhash64(a), hash(b), xxhash64(b), hash(c), xxhash64(c)
FROM test_hash_columns

-- Extra integers are hashed as children, not used as seeds.
query expect_native(hash, xxhash64)
SELECT hash(a, 0), xxhash64(a, 0), hash(a, 42), xxhash64(a, 42),
  hash(a, -1), xxhash64(a, -1) FROM test_hash_columns

-- A deterministic replacement for the generated corpus: 1000 rows with repeated strings,
-- integer/float boundaries and timestamp-like strings containing control characters.
statement
CREATE TABLE test_hash_boundaries(col STRING, a INT, b FLOAT) USING parquet

statement
INSERT INTO test_hash_boundaries
SELECT /*+ COALESCE(1) */ col, a, b FROM VALUES
  ('012345', 2147483647, float('3.4028235E38')),
  ('6789/:', -2147483648, float('-3.4028235E38')),
  ('T \t\r\n', 0, float('1.4E-45')),
  (' \t\r\n/:T', -1, float('-1.4E-45')),
  ('', 1, float('-0.0')),
  (NULL, NULL, CAST(NULL AS FLOAT)),
  ('12:34T', NULL, float('0.0')),
  (NULL, -42, float('NaN')),
  ('012345', 10, CAST(NULL AS FLOAT)),
  ('6789/:', -10, float('-Infinity'))
AS v(col, a, b) CROSS JOIN range(100)

-- FLOAT-to-string formatting has separate compatibility limits, so hash the float directly.
query expect_native(md5, hash, xxhash64, crc32, sha2, sha1)
SELECT md5(col), md5(CAST(a AS STRING)),
  hash(col), hash(col, 1), hash(col, 0), hash(col, a, b), hash(b, a, col),
  xxhash64(col), xxhash64(col, 1), xxhash64(col, 0), xxhash64(col, a, b), xxhash64(b, a, col),
  crc32(col), crc32(CAST(a AS STRING)),
  sha2(col, 0), sha2(col, 256), sha2(col, 224), sha2(col, 384), sha2(col, 512),
  sha2(col, 128), sha2(col, -1), sha1(col), sha1(CAST(a AS STRING))
FROM test_hash_boundaries

-- literal arguments
-- The SQL file test suite disables ConstantFolding, so these literal arguments reach Comet's
-- native engine as scalar values rather than being folded away by Spark's optimizer.
query
SELECT md5('Spark SQL'), sha1('test'), sha2('test', 0), sha2('test', 256), sha2('test', 224), sha2('test', 384), sha2('test', 512), sha2('test', 128), sha2('test', -1), sha2(cast(null as string), 256), hash('test'), xxhash64('test')

-- Spark hashes a float through doubleToLongBits or floatToIntBits, which canonicalize NaN, so
-- every NaN hashes alike. Negating a column flips the sign bit of a NaN, giving the bits that
-- arithmetic produces on x86-64. The infinities share a NaN's exponent bits but are not NaN, so
-- they keep their own hashes, which negation swaps.
statement
CREATE TABLE test_nan(d double, f float) USING parquet

statement
INSERT INTO test_nan VALUES (double('NaN'), float('NaN')), (0.0, 0.0), (double('-0.0'), float('-0.0')), (1.5, 1.5), (NULL, NULL), (double('Infinity'), float('Infinity')), (double('-Infinity'), float('-Infinity'))

statement
INSERT INTO test_nan VALUES (1.0, 1.0), (-1.0, -1.0), (3.14159265358979, 3.14159)

query
SELECT hash(d), hash(-d), xxhash64(d), xxhash64(-d), hash(f), hash(-f), xxhash64(f), xxhash64(-f), hash(-d, -f), xxhash64(-d, -f) FROM test_nan

query
SELECT hash(array(-d)), xxhash64(array(-d)), hash(named_struct('a', -d, 'b', -f)), xxhash64(named_struct('a', -d, 'b', -f)) FROM test_nan

-- Spark rejects hashing a map unless the legacy config above is set. Map keys cannot be null,
-- so the null row is filtered out.
query
SELECT hash(map(-d, -f)), xxhash64(map(-d, -f)) FROM test_nan WHERE d IS NOT NULL
