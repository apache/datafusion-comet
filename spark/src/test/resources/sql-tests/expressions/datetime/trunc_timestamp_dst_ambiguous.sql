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

-- Regression test for ambiguous local times during DST fall-back.
-- On 2024-11-03 at 2:00 AM America/Los_Angeles, clocks fall back to 1:00 AM,
-- so 1:30 AM occurs twice (once in PDT, once in PST). Truncating 01:30 to HOUR
-- gives 01:00, which is ambiguous. Spark's ZonedDateTime.truncatedTo keeps the
-- input's offset, so the PDT 01:30 truncates to 01:00 PDT and the PST 01:30 to
-- 01:00 PST. This used to panic natively:
-- https://github.com/apache/datafusion-comet/issues/5633

-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- Config: spark.sql.session.timeZone=America/Los_Angeles

statement
CREATE TABLE test_trunc_ambiguous(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_ambiguous VALUES
  (timestamp('2024-11-03 01:30:00')),
  (timestamp('2024-11-03 01:30:00-07:00')),
  (timestamp('2024-11-03 01:30:00-08:00')),
  (timestamp('2024-11-03 01:00:00-08:00')),
  (timestamp('1970-10-25 01:39:00-08:00'))

query
SELECT ts, date_trunc('MINUTE', ts) FROM test_trunc_ambiguous ORDER BY ts

query
SELECT ts, date_trunc('DAY', ts) FROM test_trunc_ambiguous ORDER BY ts

query
SELECT ts, date_trunc('HOUR', ts) FROM test_trunc_ambiguous ORDER BY ts

query
SELECT ts, date_trunc('WEEK', ts) FROM test_trunc_ambiguous ORDER BY ts

query
SELECT ts, date_trunc('MONTH', ts) FROM test_trunc_ambiguous ORDER BY ts

query
SELECT ts, date_trunc('QUARTER', ts) FROM test_trunc_ambiguous ORDER BY ts

query
SELECT ts, date_trunc('YEAR', ts) FROM test_trunc_ambiguous ORDER BY ts
