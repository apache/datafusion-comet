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
CREATE TABLE test_pred(a int, b int) USING parquet

statement
INSERT INTO test_pred VALUES (1, 2), (2, 2), (3, 1), (NULL, 1), (1, NULL), (NULL, NULL)

query
SELECT a = b, a <=> b, a < b, a > b, a <= b, a >= b FROM test_pred

query
SELECT a != b, NOT (a = b) FROM test_pred

query
SELECT (a > 1) AND (b > 1), (a > 1) OR (b > 1), NOT (a > 1) FROM test_pred

-- column op literal
query
SELECT a = 1, a <=> 1, a < 2, a > 0, a <= 1, a >= 2 FROM test_pred

-- literal op column
query
SELECT 2 = a, 2 < a, 0 > a, 1 <= a, 3 >= a FROM test_pred

-- literal op literal
query
SELECT 1 = 1, 1 = 2, 1 <=> NULL, NULL <=> NULL, NULL = NULL, 1 < 2, 2 > 1
