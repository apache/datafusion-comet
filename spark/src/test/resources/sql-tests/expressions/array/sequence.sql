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

-- Routes sequence through the codegen dispatcher so behavior matches Spark exactly.

statement
CREATE TABLE test_sequence(a int, b int) USING parquet

statement
INSERT INTO test_sequence VALUES (1, 5), (5, 1), (3, 3), (NULL, 5)

query
SELECT a, b, sequence(a, b) FROM test_sequence

-- literal arguments with step
query
SELECT sequence(1, 5), sequence(5, 1, -1), sequence(1, 10, 2)
