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

-- MinSparkVersion: 3.4

statement
CREATE TABLE test_equal_null(a int, b int) USING parquet

statement
INSERT INTO test_equal_null VALUES (1, 1), (1, 2), (NULL, NULL), (NULL, 1), (1, NULL)

-- equal_null: same as <=> (null-safe equality)
query
SELECT equal_null(a, b) FROM test_equal_null

-- literal arguments: both NULL
query
SELECT equal_null(NULL, NULL)

-- literal arguments: one NULL
query
SELECT equal_null(NULL, 1), equal_null(1, NULL)

-- literal arguments: equal values
query
SELECT equal_null(3, 3)

-- literal arguments: unequal values
query
SELECT equal_null(3, 4)
