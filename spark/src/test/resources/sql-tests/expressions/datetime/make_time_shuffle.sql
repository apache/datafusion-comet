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
-- Config: spark.comet.shuffle.native.partitioning.roundrobin.enabled=true
-- ConfigMatrix: spark.comet.shuffle.mode=native,jvm

statement
CREATE TABLE test_make_time_shuffle(hours int, minutes int, secs decimal(16,6)) USING parquet

statement
INSERT INTO test_make_time_shuffle VALUES (12, 30, 45.123456), (0, 0, 0.0), (23, 59, 59.999999), (1, 2, 3.5),
    (NULL, 0, 0.0), (NULL, NULL, NULL)

query
SELECT /*+ REPARTITION(3) */ make_time(hours, minutes, secs) AS t FROM test_make_time_shuffle

query
SELECT /*+ REPARTITION(3) */ hours, make_time(hours, minutes, secs) AS t, secs FROM test_make_time_shuffle

query
SELECT /*+ REPARTITION(3) */ named_struct('t', make_time(hours, minutes, secs)) AS s FROM test_make_time_shuffle

query
SELECT /*+ REPARTITION(3) */ array(make_time(hours, minutes, secs)) AS arr FROM test_make_time_shuffle

query
SELECT /*+ REPARTITION(3) */
    array(make_time(hours, minutes, secs), make_time(0, 0, 0), CAST(NULL AS TIME)) AS arr
FROM test_make_time_shuffle

query
SELECT /*+ REPARTITION(3) */
    CASE
        WHEN hours IS NULL THEN CAST(array() AS ARRAY<TIME>)
        ELSE array(make_time(hours, minutes, secs), make_time(23, 59, 59.999999))
    END AS arr
FROM test_make_time_shuffle

query
SELECT /*+ REPARTITION(3) */ map(make_time(hours, minutes, secs), hours) AS m FROM test_make_time_shuffle
WHERE hours IS NOT NULL AND minutes IS NOT NULL AND secs IS NOT NULL

query
SELECT /*+ REPARTITION(3, t) */ t
FROM (
    SELECT make_time(hours, minutes, secs) AS t
    FROM test_make_time_shuffle
)
