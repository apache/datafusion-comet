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

-- Time-window grouping. The analyzer rewrites window() grouping into a projection that uses
-- PreciseTimestampConversion (reinterpret between timestamp and long) plus a KnownNullable tag
-- around the window bounds. With those supported, batch tumbling/sliding window aggregations run
-- natively. TimestampNTZ windowing is timezone-independent, so the file runs under multiple
-- session timezones to confirm results match Spark in every zone.
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

statement
CREATE TABLE window_tbl(ts timestamp, ts_ntz timestamp_ntz) USING parquet

statement
INSERT INTO window_tbl VALUES
  (timestamp('2024-01-15 08:31:00'), cast('2024-01-15 08:31:00' as timestamp_ntz)),
  (timestamp('2024-01-15 08:37:00'), cast('2024-01-15 08:37:00' as timestamp_ntz)),
  (timestamp('2024-01-15 08:42:00'), cast('2024-01-15 08:42:00' as timestamp_ntz)),
  (timestamp('2024-01-15 09:05:00'), cast('2024-01-15 09:05:00' as timestamp_ntz)),
  (timestamp('2024-07-04 23:59:59'), cast('2024-07-04 23:59:59' as timestamp_ntz)),
  (NULL, NULL)

-- Tumbling window: Project + HashAggregate + Exchange, all native.
query
SELECT window(ts, '10 minutes').start AS s, window(ts, '10 minutes').end AS e, count(*)
FROM window_tbl GROUP BY window(ts, '10 minutes') ORDER BY s, e

-- Sliding window: overlapping windows introduce an Expand operator.
query
SELECT window(ts, '10 minutes', '5 minutes').start AS s, count(*)
FROM window_tbl GROUP BY window(ts, '10 minutes', '5 minutes') ORDER BY s

-- window_time extracts the event-time from the window struct (applied after aggregation).
query
SELECT window_time(w) AS wt, cnt FROM (
  SELECT window(ts, '10 minutes') AS w, count(*) AS cnt
  FROM window_tbl GROUP BY window(ts, '10 minutes')) ORDER BY wt

-- TimestampNTZ input: PreciseTimestampConversion also reinterprets TimestampNTZType.
query
SELECT window(ts_ntz, '10 minutes').start AS s, count(*)
FROM window_tbl GROUP BY window(ts_ntz, '10 minutes') ORDER BY s

-- session_window grouping uses UpdatingSessionsExec, which is not yet a Comet operator, so that
-- stage falls back to Spark. Results must still match.
query spark_answer_only
SELECT session_window(ts, '10 minutes').start AS s, count(*)
FROM window_tbl GROUP BY session_window(ts, '10 minutes') ORDER BY s
