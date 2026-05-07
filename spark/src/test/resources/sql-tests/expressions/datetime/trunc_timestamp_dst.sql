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

-- Exercise Comet's native date_trunc on a timezone that observes DST.
-- CometTruncTimestamp marks non-UTC as Incompatible, so enable allowIncompatible
-- to force the native path and verify correctness against Spark.

-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- Config: spark.sql.session.timeZone=America/Los_Angeles

statement
CREATE TABLE test_trunc_dst(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_dst VALUES
  (timestamp('2024-11-03 06:30:00')),
  (timestamp('2024-11-03 14:30:00')),
  (timestamp('2024-03-10 05:30:00')),
  (timestamp('2024-03-10 12:30:00')),
  (timestamp('2024-11-15 12:00:00')),
  (timestamp('2024-12-15 23:30:00')),
  (timestamp('2024-07-15 10:00:00')),
  (NULL)

-- DAY truncation on a time after fall-back (PST) produces midnight of the same
-- day, which is BEFORE the fall-back transition and should be in PDT.
query
SELECT ts, date_trunc('DAY', ts) FROM test_trunc_dst ORDER BY ts

-- HOUR truncation crossing DST boundaries.
query
SELECT ts, date_trunc('HOUR', ts) FROM test_trunc_dst ORDER BY ts

-- WEEK truncation can span DST transitions (the week of Nov 3 2024 starts Oct 28 PDT).
query
SELECT ts, date_trunc('WEEK', ts) FROM test_trunc_dst ORDER BY ts

-- MONTH truncation: Nov 15 PST truncated to MONTH gives Nov 1, which is PDT.
query
SELECT ts, date_trunc('MONTH', ts) FROM test_trunc_dst ORDER BY ts

-- QUARTER truncation: the motivating case from the PR (Dec PST -> Oct PDT).
query
SELECT ts, date_trunc('QUARTER', ts) FROM test_trunc_dst ORDER BY ts

-- YEAR truncation: Dec 15 PST truncated to YEAR gives Jan 1, which is PST too, so no DST crossing.
query
SELECT ts, date_trunc('YEAR', ts) FROM test_trunc_dst ORDER BY ts
