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
CREATE TABLE test_date_sub(d date, n int) USING parquet

statement
INSERT INTO test_date_sub VALUES (date('2024-01-15'), 1), (date('2024-01-15'), -1), (date('2024-01-15'), 0), (date('2024-01-01'), 1), (NULL, 1), (date('2024-01-15'), NULL)

query
SELECT date_sub(d, n) FROM test_date_sub

-- column + literal
query
SELECT date_sub(d, 5) FROM test_date_sub

-- literal + column
query
SELECT date_sub(date('2024-01-15'), n) FROM test_date_sub

-- literal + literal
query
SELECT date_sub(date('2024-01-15'), 5), date_sub(date('2024-01-01'), 1), date_sub(NULL, 1)
