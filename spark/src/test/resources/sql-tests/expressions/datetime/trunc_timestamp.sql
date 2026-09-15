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

-- Keep the wide-range fallback fixture independent of far-future JVM/native timezone rules.
-- Config: spark.sql.session.timeZone=UTC
-- Dictionary-encoded timestamps reuse the scalar timestamp path for dictionary values.
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_trunc_ts(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_ts VALUES
  (timestamp('2024-05-17 12:34:56.123456')),
  (timestamp('2024-02-29 23:59:59.999999')),
  (timestamp('2000-02-29 00:00:00')),
  (timestamp('1900-02-28 00:00:00')),
  (timestamp('1969-12-31 23:59:59.123456')),
  -- Valid Spark timestamp outside TimestampNanosecond's range.
  (timestamp('3333-05-17 12:34:56.123456')),
  (NULL)

query
SELECT ts, date_trunc('YEAR', ts), date_trunc('YYYY', ts), date_trunc('YY', ts) FROM test_trunc_ts ORDER BY ts

query
SELECT ts, date_trunc('QUARTER', ts) FROM test_trunc_ts ORDER BY ts

query
SELECT ts, date_trunc('MONTH', ts), date_trunc('MON', ts), date_trunc('MM', ts) FROM test_trunc_ts ORDER BY ts

query
SELECT ts, date_trunc('WEEK', ts), date_trunc('DAY', ts), date_trunc('DD', ts) FROM test_trunc_ts ORDER BY ts

query
SELECT
  ts,
  date_trunc('HOUR', ts),
  date_trunc('MINUTE', ts),
  date_trunc('SECOND', ts),
  date_trunc('MILLISECOND', ts),
  date_trunc('MICROSECOND', ts)
FROM test_trunc_ts
ORDER BY ts

query
SELECT
  ts,
  date_trunc('year', ts),
  date_trunc('Year', ts),
  date_trunc('yEaR', ts),
  date_trunc('month', ts),
  date_trunc('Mon', ts),
  date_trunc('week', ts)
FROM test_trunc_ts
ORDER BY ts

-- NULL format is Incompatible on the native path. Without allowIncompatible the
-- codegen dispatcher runs Spark's TruncTimestamp and returns NULL.
query
SELECT ts, date_trunc(NULL, ts) FROM test_trunc_ts ORDER BY ts

query
SELECT date_trunc('YEAR', NULL), date_trunc(NULL, NULL)
