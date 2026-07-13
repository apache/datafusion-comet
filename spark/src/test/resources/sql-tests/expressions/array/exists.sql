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

-- Higher-order function exists. Runs through Comet's codegen dispatcher.

statement
CREATE TABLE test_exists(a array<int>, threshold int) USING parquet

statement
INSERT INTO test_exists VALUES
  (array(1, 2, 3), 2),
  (array(-5, 5), 0),
  (array(0), 0),
  (array(), 0),
  (NULL, NULL)

-- basic
query
SELECT exists(a, x -> x > 2) FROM test_exists

-- column capture
query
SELECT exists(a, x -> x > threshold) FROM test_exists

-- predicate that can be null (array with nulls)
query
SELECT exists(array(1, NULL, 3), x -> x > 2)

-- all literals
query
SELECT exists(array(1, 2, 3), x -> x < 0)
