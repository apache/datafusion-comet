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

-- Differential coverage for scalar date_trunc around DST overlaps and gaps.
-- Explicit UTC offsets include both occurrences of the repeated US fall-back hour. The 2018
-- Sao Paulo values exercise its historic midnight spring-forward gap: truncating a valid 01:30
-- local timestamp to DAY targets the nonexistent local midnight.

-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- ConfigMatrix: spark.sql.session.timeZone=America/Los_Angeles,America/New_York,America/Sao_Paulo

statement
CREATE TABLE test_trunc_ambiguous(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_ambiguous VALUES
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
