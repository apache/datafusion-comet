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

statement
CREATE TABLE test_wb(v double) USING parquet

statement
INSERT INTO test_wb VALUES (0.0), (2.5), (5.0), (7.5), (10.0), (-1.0), (11.0), (NULL)

query
SELECT v, width_bucket(v, 0, 10, 4) FROM test_wb

query
SELECT v, width_bucket(v, 10, 0, 4) FROM test_wb

query
SELECT v, width_bucket(v, 0, 10, 1) FROM test_wb

-- literal arguments
query
SELECT width_bucket(5.0, 0, 10, 4), width_bucket(0.0, 0, 10, 4), width_bucket(NULL, 0, 10, 4)

-- day-time and year-month interval inputs take the same dispatch as doubles
query
SELECT v, width_bucket(make_dt_interval(v), make_dt_interval(0), make_dt_interval(10), 4) FROM test_wb

query
SELECT v, width_bucket(make_ym_interval(0, CAST(v AS INT)), make_ym_interval(0, 0), make_ym_interval(0, 10), 4) FROM test_wb

query
SELECT width_bucket(INTERVAL '2' DAY, INTERVAL '0' DAY, INTERVAL '10' DAY, 5), width_bucket(INTERVAL '2' YEAR, INTERVAL '0' YEAR, INTERVAL '10' YEAR, 5), width_bucket(CAST(NULL AS INTERVAL DAY), INTERVAL '0' DAY, INTERVAL '10' DAY, 5)
