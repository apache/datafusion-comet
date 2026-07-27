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

-- The one argument uuid(seed) form only exists in Spark 4.0+. With a fixed seed the output is
-- deterministic, so Comet must reproduce Spark's output exactly. Comet drives the same Commons
-- Math3 MersenneTwister as org.apache.spark.sql.catalyst.util.RandomUUIDGenerator, combining the
-- seed with the partition index, so these queries assert bit-for-bit equality with Spark in the
-- default query mode.

-- MinSparkVersion: 4.0

statement
CREATE TABLE test_uuid_seed(id int) USING parquet

statement
INSERT INTO test_uuid_seed VALUES (1), (2), (3), (4), (5)

-- Multi-partition table used by the DISTRIBUTE BY query below to exercise partitionIndex != 0.
statement
CREATE TABLE test_uuid_parts(id int) USING parquet

statement
INSERT INTO test_uuid_parts SELECT id FROM range(0, 32)

-- fixed seed, single row -- also exercises the no-scan (OneRowRelation) planning path
query
SELECT uuid(0)

-- pin the no-scan path with a deterministic assertion on top of the raw value above
query
SELECT length(uuid(0))

-- fixed seed, multiple rows: the generator advances per row within the partition
query
SELECT uuid(42) FROM test_uuid_seed

-- Forces a hash exchange so uuid runs post-shuffle across several partitions. Locks the
-- `seed + partitionIndex` offset used by RandomUUIDGenerator; a single-partition test would
-- silently agree with Spark even if Comet ignored partitionIndex.
query
SELECT uuid(42) FROM test_uuid_parts DISTRIBUTE BY id

-- Aliased seeded projections: both nodes carry the same explicit seed, so freshCopyIfContainsStatefulExpression
-- must not accidentally hoist a shared instance -- each row must satisfy uuid(0) = uuid(0).
query
SELECT uuid(0) = uuid(0) FROM test_uuid_seed

-- zero seed over multiple rows
query
SELECT uuid(0) FROM test_uuid_seed

-- negative seed
query
SELECT uuid(-12345) FROM test_uuid_seed

-- Long.MinValue and Long.MaxValue seeds
query
SELECT uuid(-9223372036854775808) FROM test_uuid_seed

query
SELECT uuid(9223372036854775807) FROM test_uuid_seed

-- seeded uuid combined with other expressions stays native and deterministic
query
SELECT upper(uuid(7)), length(uuid(7)) FROM test_uuid_seed
