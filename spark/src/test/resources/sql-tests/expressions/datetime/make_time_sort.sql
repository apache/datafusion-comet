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
CREATE TABLE test_make_time_sort(id int, hours int, minutes int, secs decimal(16,6)) USING parquet

statement
INSERT INTO test_make_time_sort VALUES
    (1, 12, 30, 45.123456),
    (2, 0, 0, 0.000000),
    (3, 23, 59, 59.999999),
    (4, 1, 2, 3.500000),
    (5, 0, 0, 0.000001),
    (6, NULL, NULL, NULL),
    (7, 0, 0, 0.000000)

query
SELECT id, t
FROM (
    SELECT id, make_time(hours, minutes, secs) AS t
    FROM test_make_time_sort
)
SORT BY t ASC NULLS FIRST

query
SELECT id, t
FROM (
    SELECT id, make_time(hours, minutes, secs) AS t
    FROM test_make_time_sort
)
SORT BY t DESC NULLS LAST

query
SELECT id, t
FROM (
    SELECT id, make_time(hours, minutes, secs) AS t
    FROM test_make_time_sort
)
SORT BY t ASC NULLS LAST

query
SELECT id, t
FROM (
    SELECT id, make_time(hours, minutes, secs) AS t
    FROM test_make_time_sort
)
SORT BY t DESC NULLS FIRST

query
SELECT id, t
FROM (
    SELECT id, make_time(hours, minutes, secs) AS t
    FROM test_make_time_sort
)
SORT BY t ASC NULLS LAST, id DESC
