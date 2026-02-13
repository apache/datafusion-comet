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
CREATE TABLE test_next_day(d date) USING parquet

statement
INSERT INTO test_next_day VALUES (date('2023-01-01')), (date('2024-02-29')), (date('1969-12-31')), (date('2024-06-15')), (NULL)

-- full day names
query
SELECT next_day(d, 'Sunday') FROM test_next_day

query
SELECT next_day(d, 'Monday') FROM test_next_day

query
SELECT next_day(d, 'Tuesday') FROM test_next_day

query
SELECT next_day(d, 'Wednesday') FROM test_next_day

query
SELECT next_day(d, 'Thursday') FROM test_next_day

query
SELECT next_day(d, 'Friday') FROM test_next_day

query
SELECT next_day(d, 'Saturday') FROM test_next_day

-- abbreviated day names
query
SELECT next_day(d, 'Sun') FROM test_next_day

query
SELECT next_day(d, 'Mon') FROM test_next_day

query
SELECT next_day(d, 'Tue') FROM test_next_day

query
SELECT next_day(d, 'Wed') FROM test_next_day

query
SELECT next_day(d, 'Thu') FROM test_next_day

query
SELECT next_day(d, 'Fri') FROM test_next_day

query
SELECT next_day(d, 'Sat') FROM test_next_day

-- literal arguments
query
SELECT next_day(date('2023-01-01'), 'Monday'), next_day(date('2023-01-01'), 'Sunday')

-- null handling
query
SELECT next_day(NULL, 'Monday'), next_day(date('2023-01-01'), NULL)
