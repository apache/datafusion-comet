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

-- Higher-order function forall. Runs through Comet's codegen dispatcher.

statement
CREATE TABLE test_forall(a array<int>, threshold int) USING parquet

statement
INSERT INTO test_forall VALUES
  (array(1, 2, 3), 0),
  (array(-5, 5), 0),
  (array(0), 0),
  (array(), 0),
  (NULL, NULL)

-- basic
query
SELECT forall(a, x -> x > 0) FROM test_forall

-- column capture
query
SELECT forall(a, x -> x >= threshold) FROM test_forall

-- predicate that can be null (array with nulls)
query
SELECT forall(array(2, NULL, 4), x -> x > 0)

-- all literals
query
SELECT forall(array(1, 2, 3), x -> x > 0)
