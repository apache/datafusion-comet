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
CREATE TABLE test_if(cond boolean, a int, b int) USING parquet

statement
INSERT INTO test_if VALUES (true, 1, 2), (false, 1, 2), (NULL, 1, 2), (true, NULL, 2), (false, 1, NULL)

query
SELECT IF(cond, a, b) FROM test_if

query
SELECT IF(a > 0, 'positive', 'non-positive') FROM test_if

-- literal arguments
query
SELECT IF(true, 1, 2), IF(false, 1, 2), IF(NULL, 1, 2)
