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

-- hash functions
statement
CREATE TABLE test(col string, a int, b float) USING parquet

statement
INSERT INTO test VALUES ('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999), ('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999)

query
SELECT md5(col), md5(cast(a as string)), md5(cast(b as string)), hash(col), hash(col, 1), hash(col, 0), hash(col, a, b), hash(b, a, col), xxhash64(col), xxhash64(col, 1), xxhash64(col, 0), xxhash64(col, a, b), xxhash64(b, a, col), sha2(col, 0), sha2(col, 256), sha2(col, 224), sha2(col, 384), sha2(col, 512), sha2(col, 128), sha2(col, -1), sha1(col), sha1(cast(a as string)), sha1(cast(b as string)) FROM test

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

query
SELECT hash(d), hash(-d), xxhash64(d), xxhash64(-d), hash(f), hash(-f), xxhash64(f), xxhash64(-f), hash(-d, -f), xxhash64(-d, -f) FROM test_nan

query
SELECT hash(array(-d)), xxhash64(array(-d)), hash(named_struct('a', -d, 'b', -f)), xxhash64(named_struct('a', -d, 'b', -f)) FROM test_nan

-- Spark rejects hashing a map unless the legacy config above is set. Map keys cannot be null,
-- so the null row is filtered out.
query
SELECT hash(map(-d, -f)), xxhash64(map(-d, -f)) FROM test_nan WHERE d IS NOT NULL
