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

-- Differential coverage for scalar date_trunc around DST overlaps, gaps, and historical offsets
-- containing seconds. Explicit UTC offsets include both occurrences of the repeated US fall-back
-- hour. The 2018 Sao Paulo values exercise its historic midnight spring-forward gap: truncating a
-- valid 01:30 local timestamp to DAY targets the nonexistent local midnight. Africa/Monrovia used
-- UTC-00:44:30 until 1972, so its local minute boundaries do not align with UTC minute boundaries.
-- Asia/Aden covers minute truncation across a historical offset transition. Havana repeated
-- midnight on 2020-11-01, which makes MONTH select the earlier UTC occurrence. Toronto
-- skipped 1919-03-30 23:30 through 1919-03-31 00:30, so WEEK must select the gap end.
-- Asuncion skipped midnight on 2023-10-01, a MONTH and QUARTER boundary.

-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- Config: spark.sql.parquet.int96RebaseModeInWrite=CORRECTED
-- ConfigMatrix: spark.sql.session.timeZone=America/Los_Angeles,America/New_York,America/Sao_Paulo,Africa/Monrovia,Asia/Aden,America/Havana,America/Toronto,America/Asuncion

statement
CREATE TABLE test_trunc_ambiguous(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_ambiguous VALUES
  (timestamp('1919-04-02T16:00:00Z')),
  (timestamp('1919-03-31T04:30:00Z')),
  (timestamp('2023-10-15T12:00:00Z')),
  (TIMESTAMP '1960-06-15 10:30:45'),
  (timestamp('1947-03-13T20:53:30.123Z')),
  (timestamp('1972-01-07T00:44:45Z')),
  (timestamp('2018-11-04T01:30:15.123456Z')),
  (timestamp('2018-11-04T02:30:15.123456Z')),
  (timestamp('2018-11-04T03:30:15.123456Z')),
  (timestamp('2018-11-04T04:30:15.123456Z')),
  (timestamp('2024-03-10T06:30:15.123456Z')),
  (timestamp('2024-03-10T07:30:15.123456Z')),
  (timestamp('2024-03-10T08:30:15.123456Z')),
  (timestamp('2024-03-10T09:30:15.123456Z')),
  (timestamp('2024-03-10T10:30:15.123456Z')),
  (timestamp('2024-03-10T11:30:15.123456Z')),
  (timestamp('2024-11-03T05:30:15.123456Z')),
  (timestamp('2024-11-03T06:30:15.123456Z')),
  (timestamp('2024-11-03T07:30:15.123456Z')),
  (timestamp('2024-11-03T08:30:15.123456Z')),
  (timestamp('2024-11-03T09:30:15.123456Z')),
  (timestamp('2024-11-03T10:30:15.123456Z')),
  (timestamp('2020-11-15T12:00:00Z')),
  (NULL)

query
SELECT
  ts,
  date_trunc('YEAR', ts),
  date_trunc('QUARTER', ts),
  date_trunc('MONTH', ts),
  date_trunc('WEEK', ts),
  date_trunc('DAY', ts),
  date_trunc('HOUR', ts),
  date_trunc('MINUTE', ts),
  date_trunc('SECOND', ts),
  date_trunc('MILLISECOND', ts),
  date_trunc('MICROSECOND', ts)
FROM test_trunc_ambiguous
ORDER BY ts
