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

-- Routes to_char through the codegen dispatcher so behavior matches Spark exactly.

-- MinSparkVersion: 3.5

statement
CREATE TABLE test_to_char(v decimal(10,2)) USING parquet

statement
INSERT INTO test_to_char VALUES (454.00), (78.12), (0.00), (NULL)

query
SELECT v, to_char(v, '0000.00') FROM test_to_char

-- literal arguments
query
SELECT to_char(454, '999'), to_char(78.12, '99.99')
