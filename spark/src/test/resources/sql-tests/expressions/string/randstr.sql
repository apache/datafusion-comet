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

-- randstr(length, seed) only exists in Spark 4.0+. With a fixed seed the output is deterministic,
-- so Comet must reproduce Spark's output exactly. Comet drives the same XORShiftRandom as
-- org.apache.spark.sql.catalyst.expressions.ExpressionImplUtils.randStr, combining the seed with
-- the partition index, so these queries assert bit-for-bit equality with Spark in the default
-- query mode. The no-seed randstr(length) form draws a fresh random seed per planning pass, so it
-- can only be checked for deterministic properties (length, alphabet).

-- MinSparkVersion: 4.0

statement
CREATE TABLE test_randstr(id int) USING parquet

statement
INSERT INTO test_randstr VALUES (1), (2), (3), (4), (5)

-- ===== Seeded form: bit-for-bit equality with Spark =====

-- fixed seed, single row
query
SELECT randstr(10, 0)

-- fixed seed, multiple rows: the generator advances per row within the partition
query
SELECT randstr(10, 42) FROM test_randstr

-- negative seed
query
SELECT randstr(8, -12345) FROM test_randstr

-- zero length yields the empty string
query
SELECT randstr(0, 42) FROM test_randstr

-- length one
query
SELECT randstr(1, 7) FROM test_randstr

-- longer string
query
SELECT randstr(64, 123456789)

-- Long.MinValue and Long.MaxValue seeds
query
SELECT randstr(12, -9223372036854775808) FROM test_randstr

query
SELECT randstr(12, 9223372036854775807) FROM test_randstr

-- combined with other expressions stays native and deterministic
query
SELECT upper(randstr(6, 7)), length(randstr(6, 7)) FROM test_randstr

-- Int vs Long seed: the serde sign-extends an Int seed with `toLong`, so an Int literal and the
-- equivalent Long literal must drive the same generator and produce identical strings. (A literal,
-- not `cast(... as bigint)`, so the seed stays foldable and randstr runs natively.)
query
SELECT randstr(8, -1) = randstr(8, -1L) FROM test_randstr

-- randstr feeding a filter: computed natively in the inner projection, then filtered on the
-- projected column. The nondeterministic column blocks predicate pushdown, so randstr stays in the
-- native Project; the seeded generator makes the result reproducible, so Comet and Spark select the
-- same rows.
query
SELECT id FROM (SELECT id, randstr(1, 7) AS r FROM test_randstr) t WHERE r RLIKE '^[a-m]$'

-- negative length: Comet falls back to Spark, which raises INVALID_PARAMETER_VALUE.LENGTH
query expect_error(INVALID_PARAMETER_VALUE)
SELECT randstr(-1, 0)

-- ===== No-seed form: deterministic properties (identical on both engines) =====

-- length is always the requested value
query
SELECT length(randstr(16)) FROM test_randstr

-- only alphanumeric characters are produced
query
SELECT randstr(20) RLIKE '^[0-9a-zA-Z]{20}$' FROM test_randstr
