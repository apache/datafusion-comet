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

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_window_time(time timestamp, value int) USING parquet

statement
INSERT INTO test_window_time VALUES (timestamp('2023-01-01 12:00:00'), 1), (timestamp('2023-01-01 12:05:00'), 2), (timestamp('2023-01-01 12:15:00'), 3)

-- basic window_time with tumbling window
query spark_answer_only
SELECT max(window_time(window)), sum(value) FROM (SELECT window(time, '10 minutes') AS window, value FROM test_window_time) GROUP BY window

-- window_time with sliding window
query spark_answer_only
SELECT max(window_time(window)), count(value) FROM (SELECT window(time, '10 minutes', '5 minutes') AS window, value FROM test_window_time) GROUP BY window
