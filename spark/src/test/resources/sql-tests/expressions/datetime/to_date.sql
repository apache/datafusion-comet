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

-- to_date function
statement
CREATE TABLE test_to_date(col STRING) USING parquet

statement
INSERT INTO test_to_date VALUES ('2026-01-30'), ('2026-03-10'), (NULL)

query
SELECT col, to_date(col) FROM test_to_date

                                  statement
CREATE TABLE test_to_date_fmt(col STRING) USING parquet

statement
INSERT INTO test_to_date_fmt VALUES ('2026/01/30'), ('2026/03/10'), (NULL)

query
SELECT col, to_date(col, 'yyyy/MM/dd') FROM test_to_date_fmt

query
SELECT to_date('2026-01-30')

query
SELECT to_date('2026/01/30', 'yyyy/MM/dd')

query
SELECT to_date('2026-01-30 10:30:00')
