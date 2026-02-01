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

-- DatePart functions
statement
CREATE TABLE test_dt(col timestamp) USING parquet

statement
INSERT INTO test_dt VALUES (timestamp('2024-06-15 10:30:00')), (timestamp('1900-01-01')), (null)

query
SELECT col, year(col), month(col), day(col), weekday(col), dayofweek(col), dayofyear(col), weekofyear(col), quarter(col) FROM test_dt

query
SELECT hour(col), minute(col), second(col) FROM test_dt

-- Midnight and end-of-day
statement
CREATE TABLE test_dt_hms(ts timestamp) USING parquet

statement
INSERT INTO test_dt_hms VALUES (timestamp('2024-01-01 00:00:00')), (timestamp('2024-01-01 23:59:59')), (timestamp('2024-06-15 12:30:45')), (NULL)

query
SELECT hour(ts), minute(ts), second(ts) FROM test_dt_hms
