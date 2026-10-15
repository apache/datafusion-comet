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

-- approx_count_distinct on TimestampType must be timezone-independent: the hashed value is the
-- stored UTC microseconds, so results match Spark under a non-UTC session time zone.

-- Config: spark.sql.session.timeZone=America/Los_Angeles

statement
CREATE TABLE acd_tz(ts timestamp, grp int) USING parquet

statement
INSERT INTO acd_tz VALUES
  (TIMESTAMP '2024-01-01 00:00:00', 0),
  (TIMESTAMP '2024-06-01 12:30:00', 0),
  (TIMESTAMP '2024-01-01 00:00:00', 1),
  (TIMESTAMP '2024-12-31 23:59:59', 1),
  (NULL, 1)

-- global aggregation under a non-UTC session time zone
query
SELECT approx_count_distinct(ts) FROM acd_tz

-- grouped aggregation under a non-UTC session time zone
query
SELECT grp, approx_count_distinct(ts) FROM acd_tz GROUP BY grp ORDER BY grp
