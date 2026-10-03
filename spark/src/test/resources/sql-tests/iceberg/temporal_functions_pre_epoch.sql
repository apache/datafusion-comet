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

-- Iceberg's years, months, days, and hours system functions on timestamps just after a pre-1970
-- unit boundary (https://github.com/apache/datafusion-comet/issues/6426). Iceberg's DateTimeUtil
-- places a pre-1970 timestamp whose microsecond of second is 999999 by the second before it, so
-- right after a boundary it gets the unit before: 1969-01-01 00:00:00.999999 is in year -2,
-- month -13, day 1968-12-31, and hour -8761, where a floor gives -1, -12, 1969-01-01, and -8760.
-- Each query also runs with Comet off, so the expected values come from Iceberg's own functions.
-- CometIcebergSystemFunctionSuite writes the same timestamps with the native Iceberg writer.

-- No Iceberg spark-runtime is published for Spark 4.2 yet, and the 4.0 runtime the build reuses
-- is binary-incompatible with it, which is also why CometIcebergTestBase reports Iceberg as
-- unavailable there. See https://github.com/apache/datafusion-comet/issues/4969.
-- MaxSparkVersion: 4.1

-- Config: spark.sql.catalog.test_cat=org.apache.iceberg.spark.SparkCatalog
-- Config: spark.sql.catalog.test_cat.type=hadoop
-- Config: spark.sql.catalog.test_cat.warehouse=/tmp/comet-iceberg-sql-test
-- UTC, so that the TIMESTAMP values sit on the same boundaries as the TIMESTAMP_NTZ ones.
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS

-- A parquet table rather than an Iceberg one, so that no scan absorbs a filter. Rows 1 to 4 lie
-- 999999 microseconds after a year, a month, a day, and an hour boundary (a year boundary is also
-- a month, day, and hour boundary, and so on). Row 5 sits inside the hour Iceberg gives row 4,
-- and row 6 inside the day it gives row 1, both away from any boundary. Row 7 is after the epoch,
-- where Iceberg floors.
statement
CREATE TABLE iceberg_pre_epoch (id INT, ts TIMESTAMP, ntz TIMESTAMP_NTZ) USING parquet

statement
INSERT INTO iceberg_pre_epoch VALUES
  (1, TIMESTAMP '1969-01-01 00:00:00.999999', TIMESTAMP_NTZ '1969-01-01 00:00:00.999999'),
  (2, TIMESTAMP '1969-12-01 00:00:00.999999', TIMESTAMP_NTZ '1969-12-01 00:00:00.999999'),
  (3, TIMESTAMP '1969-12-31 00:00:00.999999', TIMESTAMP_NTZ '1969-12-31 00:00:00.999999'),
  (4, TIMESTAMP '1969-12-31 23:00:00.999999', TIMESTAMP_NTZ '1969-12-31 23:00:00.999999'),
  (5, TIMESTAMP '1969-12-31 22:30:00', TIMESTAMP_NTZ '1969-12-31 22:30:00'),
  (6, TIMESTAMP '1968-12-31 12:00:00', TIMESTAMP_NTZ '1968-12-31 12:00:00'),
  (7, TIMESTAMP '1970-01-01 01:00:00.999999', TIMESTAMP_NTZ '1970-01-01 01:00:00.999999'),
  (8, NULL, NULL)

-- Every query names staticinvoke, the node Spark plans an Iceberg system function call as, in
-- expect_native. A call Comet did not lower to its own kernel would run Iceberg's Java code through
-- the codegen dispatcher and match Spark whatever the kernel returns.
query expect_native(staticinvoke)
SELECT id,
  test_cat.system.years(ts), test_cat.system.months(ts),
  test_cat.system.days(ts), test_cat.system.hours(ts),
  test_cat.system.years(ntz), test_cat.system.months(ntz),
  test_cat.system.days(ntz), test_cat.system.hours(ntz)
FROM iceberg_pre_epoch

-- Rows 1 and 6. A floor would leave out row 1 in these three.
query expect_native(staticinvoke)
SELECT id FROM iceberg_pre_epoch WHERE test_cat.system.years(ts) = -2

query expect_native(staticinvoke)
SELECT id FROM iceberg_pre_epoch WHERE test_cat.system.months(ts) = -13

query expect_native(staticinvoke)
SELECT id FROM iceberg_pre_epoch WHERE test_cat.system.days(ts) = DATE '1968-12-31'

-- Rows 4 and 5. A floor would leave out row 4.
query expect_native(staticinvoke)
SELECT id FROM iceberg_pre_epoch WHERE test_cat.system.hours(ts) = -2
