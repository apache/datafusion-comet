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
-- MaxSparkVersion: 4.1
-- Config: spark.sql.timeType.enabled=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

-- Spark 4.1 returns Decimal(8,6); later releases use precision-dependent result types.
statement
CREATE TABLE test_extract_time(s STRING) USING parquet

statement
INSERT INTO test_extract_time VALUES
  ('00:00:00'),
  ('00:00:00.000001'),
  ('00:01:00'),
  ('12:30:45.123456789'),
  ('23:59:59.999999'),
  ('T12'),
  ('T12 AM'),
  ('12:30:45.'),
  (NULL)

-- No JVM dispatcher: both parsing and fractional-second extraction must execute in Rust.
query
SELECT extract(second from to_time(s)), extract(second from try_to_time(s))
FROM test_extract_time

-- The result is Decimal(8,6) for all input precisions, including trailing zeroes.
-- Literal casts establish TIME(p) inputs without depending on native TIME-to-TIME casts.
query
SELECT
  extract(second from cast(TIME '12:30:45.123456' as TIME(0))),
  extract(second from cast(TIME '12:30:45.123456' as TIME(1))),
  extract(second from cast(TIME '12:30:45.123456' as TIME(2))),
  extract(second from cast(TIME '12:30:45.123456' as TIME(3))),
  extract(second from cast(TIME '12:30:45.123456' as TIME(4))),
  extract(second from cast(TIME '12:30:45.123456' as TIME(5))),
  extract(second from cast(TIME '12:30:45.123456' as TIME(6)))
FROM test_extract_time

-- Invalid strings become NULL before extraction.
query
SELECT extract(second from try_to_time('T24')),
  extract(second from try_to_time('12:30:45..')),
  extract(second from cast(NULL as TIME(6)))
FROM test_extract_time
