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

-- Higher-order function aggregate (a.k.a. reduce). Runs through Comet's codegen dispatcher.

statement
CREATE TABLE test_aggregate(a array<int>, start int) USING parquet

statement
INSERT INTO test_aggregate VALUES
  (array(1, 2, 3), 0),
  (array(-5, 5), 100),
  (array(10), 0),
  (array(), 0),
  (NULL, NULL)

-- basic sum
query
SELECT aggregate(a, 0, (acc, x) -> acc + x) FROM test_aggregate

-- column capture in the initial value
query
SELECT aggregate(a, start, (acc, x) -> acc + x) FROM test_aggregate

-- with a finish function
query
SELECT aggregate(a, 0, (acc, x) -> acc + x, acc -> acc * 10) FROM test_aggregate

-- all literals
query
SELECT aggregate(array(1, 2, 3, 4), 0, (acc, x) -> acc + x)
