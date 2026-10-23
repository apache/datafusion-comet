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

-- min/max over a struct with timestamp and date fields under several session time zones,
-- including values near the ends of the range and DST transitions. 1901 stays clear of the
-- INT96 rebase cutoff in every zone listed.

-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles,Asia/Kolkata,Australia/Lord_Howe

statement
CREATE TABLE test_mm_struct_tz(g int, t timestamp, dt date, s string) USING parquet

statement
INSERT INTO test_mm_struct_tz VALUES
  (1, TIMESTAMP'1901-01-01 00:00:00', DATE'1901-01-01', 'a'),
  (1, TIMESTAMP'9999-12-30 23:59:59.999999', DATE'9999-12-31', 'b'),
  (1, TIMESTAMP'1970-01-01 00:00:00', DATE'1970-01-01', 'c'),
  (2, TIMESTAMP'2024-03-10 01:59:59', DATE'2024-03-10', 'before'),
  (2, TIMESTAMP'2024-03-10 03:00:00', DATE'2024-03-10', 'after'),
  (2, TIMESTAMP'2024-11-03 01:30:00', DATE'2024-11-03', 'overlap'),
  (3, TIMESTAMP'1969-12-31 23:59:59.999999', DATE'1969-12-31', '한국어'),
  (3, NULL, NULL, NULL)

query
SELECT g,
  min(struct(coalesce(t, TIMESTAMP'2000-01-01 00:00:00') AS t, coalesce(dt, DATE'2000-01-01') AS dt, coalesce(s, '') AS s)),
  max(struct(coalesce(t, TIMESTAMP'2000-01-01 00:00:00') AS t, coalesce(dt, DATE'2000-01-01') AS dt, coalesce(s, '') AS s))
FROM test_mm_struct_tz GROUP BY g ORDER BY g

-- The date leads, so ties on the date are decided by the timestamp.
query
SELECT g,
  min(struct(coalesce(dt, DATE'2000-01-01') AS dt, coalesce(t, TIMESTAMP'2000-01-01 00:00:00') AS t)),
  max(struct(coalesce(dt, DATE'2000-01-01') AS dt, coalesce(t, TIMESTAMP'2000-01-01 00:00:00') AS t))
FROM test_mm_struct_tz GROUP BY g ORDER BY g
