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
CREATE TABLE test_current_timezone(id INT) USING parquet

statement
INSERT INTO test_current_timezone VALUES (1), (2), (3), (4), (NULL)

query
SELECT current_timezone()

query
SELECT current_timezone() IS NOT NULL

query
SELECT current_timezone() = current_timezone()

query
SELECT length(current_timezone()) > 0

query
SELECT id, current_timezone() IS NOT NULL FROM test_current_timezone

query
SELECT id FROM test_current_timezone WHERE current_timezone() = current_timezone()
