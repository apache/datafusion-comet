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

-- MinSparkVersion: 4.1
-- Config: spark.sql.timeType.enabled=true

statement
CREATE TABLE test_make_time_min_max(grp string, hours int, minutes int, secs decimal(16,6)) USING parquet

statement
INSERT INTO test_make_time_min_max VALUES
    ('a', 12, 30, 45.123456),
    ('a', 0, 0, 0.000000),
    ('a', NULL, NULL, NULL),
    ('b', 23, 59, 59.999999),
    ('b', 1, 2, 3.500000),
    ('c', NULL, 0, 0.000000),
    ('c', 12, NULL, 0.000000),
    ('c', 12, 30, NULL)

query
SELECT
    min(make_time(hours, minutes, secs)) AS min_time,
    max(make_time(hours, minutes, secs)) AS max_time
FROM test_make_time_min_max

query
SELECT
    grp,
    min(make_time(hours, minutes, secs)) AS min_time,
    max(make_time(hours, minutes, secs)) AS max_time
FROM test_make_time_min_max
GROUP BY grp
ORDER BY grp

query
SELECT
    min(make_time(hours, minutes, secs)) AS min_time,
    max(make_time(hours, minutes, secs)) AS max_time
FROM test_make_time_min_max
WHERE grp = 'c'
