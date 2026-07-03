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

-- MinSparkVersion: 4.1
-- Config: spark.sql.timeType.enabled=true

-- Casts to/from TIME have full Spark codegen but no native lowering in Comet. CometCast
-- routes them through the JVM codegen dispatcher so the enclosing projection stays
-- native and results match Spark exactly.

statement
CREATE TABLE test_cast_time(s string, h int, m int, sec decimal(16,6)) USING parquet

statement
INSERT INTO test_cast_time VALUES
  ('00:00:00',        0,  0, 0.000000),
  ('12:34:56',       12, 34, 56.000000),
  ('23:59:59.999999',23, 59, 59.999999),
  ('01:00:00',        1,  0, 0.000000),
  (NULL, NULL, NULL, NULL)

-- string -> TIME (column)
query
SELECT CAST(s AS TIME) FROM test_cast_time

-- string -> TIME (literals)
query
SELECT CAST('00:00:00' AS TIME)

query
SELECT CAST('23:59:59.999999' AS TIME)

-- TIME -> string (column)
query
SELECT CAST(make_time(h, m, sec) AS STRING) FROM test_cast_time

-- TIME -> string (literals)
query
SELECT CAST(TIME '00:00:00' AS STRING)

query
SELECT CAST(TIME '13:45:07.123456' AS STRING)

-- TIME -> BIGINT (whole seconds since midnight, via floor(nanos / 1e9))
query
SELECT CAST(make_time(h, m, sec) AS BIGINT) FROM test_cast_time

query
SELECT CAST(TIME '00:00:00' AS BIGINT)

query
SELECT CAST(TIME '01:00:00' AS BIGINT)

query
SELECT CAST(TIME '23:59:59.999999' AS BIGINT)

-- TIME -> INT (same seconds-truncation semantics)
query
SELECT CAST(TIME '13:45:07.999999' AS INT)

-- TIME -> DECIMAL (fractional-seconds-preserving, nanos-based)
query
SELECT CAST(TIME '13:45:07.123456' AS DECIMAL(18,6))

-- TIME -> TIME with different precision (truncates fractional digits)
query
SELECT CAST(TIME '13:45:07.123456' AS TIME(0))

query
SELECT CAST(TIME '13:45:07.123456' AS TIME(3))

-- NULL round-trips
query
SELECT CAST(CAST(NULL AS STRING) AS TIME)

query
SELECT CAST(CAST(NULL AS TIME) AS STRING)
