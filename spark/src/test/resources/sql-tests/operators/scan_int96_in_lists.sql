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

-- DataFusion's INT96 coercion finds a leaf by a Parquet path that assumes the standard `list` /
-- `element` layout. A leaf in a legacy list layout (`bag` / `array`, which Spark writes with
-- writeLegacyFormat=true) was never coerced, so a value outside the nanosecond range (1677 to
-- 2262) wrapped to a wrong timestamp with no error
-- (https://github.com/apache/datafusion-comet/issues/7029).
--
-- The files are written CORRECTED, so Spark reads them as written under either INT96 read mode.
-- A file written LEGACY is not covered: the native scan does not rebase INT96, in a list or not
-- (https://github.com/apache/datafusion-comet/issues/5010).
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.parquet.outputTimestampType=INT96
-- Config: spark.sql.parquet.int96RebaseModeInWrite=CORRECTED
-- ConfigMatrix: spark.sql.parquet.writeLegacyFormat=true,false
-- ConfigMatrix: spark.sql.parquet.int96RebaseModeInRead=LEGACY,CORRECTED

statement
CREATE TABLE test_int96_lists(
  id int,
  arr array<timestamp>,
  arr_arr array<array<timestamp>>,
  arr_struct array<struct<ts: timestamp>>,
  map_arr map<string, array<timestamp>>) USING parquet

-- Ids 0 to 4: 9999-12-31, 1600-01-01 and 2263-01-01 are outside the nanosecond range, 1000-01-01
-- is also before the 1582 calendar cutover, and 2020-01-01 is inside the range.
statement
INSERT INTO test_int96_lists VALUES
  (0,
   array(timestamp('9999-12-31 23:59:59')),
   array(array(timestamp('9999-12-31 23:59:59'))),
   array(named_struct('ts', timestamp('9999-12-31 23:59:59'))),
   map('k', array(timestamp('9999-12-31 23:59:59')))),
  (1,
   array(timestamp('1600-01-01 00:00:00')),
   array(array(timestamp('1600-01-01 00:00:00'))),
   array(named_struct('ts', timestamp('1600-01-01 00:00:00'))),
   map('k', array(timestamp('1600-01-01 00:00:00')))),
  (2,
   array(timestamp('2263-01-01 00:00:00')),
   array(array(timestamp('2263-01-01 00:00:00'))),
   array(named_struct('ts', timestamp('2263-01-01 00:00:00'))),
   map('k', array(timestamp('2263-01-01 00:00:00')))),
  (3,
   array(timestamp('1000-01-01 00:00:00')),
   array(array(timestamp('1000-01-01 00:00:00'))),
   array(named_struct('ts', timestamp('1000-01-01 00:00:00'))),
   map('k', array(timestamp('1000-01-01 00:00:00')))),
  (4,
   array(timestamp('2020-01-01 00:00:00')),
   array(array(timestamp('2020-01-01 00:00:00'))),
   array(named_struct('ts', timestamp('2020-01-01 00:00:00'))),
   map('k', array(timestamp('2020-01-01 00:00:00')))),
  -- NULL at every level, then empty arrays and maps, then NULL elements and NULL map values.
  (5,
   CAST(NULL AS array<timestamp>),
   CAST(NULL AS array<array<timestamp>>),
   CAST(NULL AS array<struct<ts: timestamp>>),
   CAST(NULL AS map<string, array<timestamp>>)),
  (6,
   CAST(array() AS array<timestamp>),
   CAST(array() AS array<array<timestamp>>),
   CAST(array() AS array<struct<ts: timestamp>>),
   CAST(map() AS map<string, array<timestamp>>)),
  (7,
   array(CAST(NULL AS timestamp), timestamp('9999-12-31 23:59:59')),
   array(CAST(NULL AS array<timestamp>), array(CAST(NULL AS timestamp), timestamp('1600-01-01 00:00:00'))),
   array(CAST(NULL AS struct<ts: timestamp>), named_struct('ts', CAST(NULL AS timestamp)),
     named_struct('ts', timestamp('2263-01-01 00:00:00'))),
   map('a', CAST(NULL AS array<timestamp>), 'b', array(timestamp('9999-12-31 23:59:59'), CAST(NULL AS timestamp))))

query
SELECT id, arr, arr_arr, arr_struct, map_arr FROM test_int96_lists ORDER BY id

-- Column pruning keeps the other columns' leaves out of the read.
query
SELECT id, arr_arr FROM test_int96_lists ORDER BY id

query
SELECT id, arr FROM test_int96_lists WHERE id >= 5 ORDER BY id
