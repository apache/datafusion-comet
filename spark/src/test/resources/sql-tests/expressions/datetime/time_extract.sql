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
CREATE TABLE test_time_extract(hours int, minutes int, secs DECIMAL(16, 6)) USING parquet

statement
INSERT INTO test_time_extract VALUES
    (0, 0, 0.000000),
    (12, 30, 45.123456),
    (23, 59, 59.999999),
    (NULL, NULL, NULL)

query
SELECT hour(make_time(hours, minutes, secs))
FROM test_time_extract

query
SELECT minute(make_time(hours, minutes, secs))
FROM test_time_extract

query
SELECT second(make_time(hours, minutes, secs))
FROM test_time_extract

query
SELECT extract(SECOND FROM make_time(hours, minutes, secs))
FROM test_time_extract

query
SELECT date_part('SECOND', make_time(hours, minutes, secs))
FROM test_time_extract
