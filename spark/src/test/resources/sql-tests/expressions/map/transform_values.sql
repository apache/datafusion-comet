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

-- Higher-order function transform_values. Runs through Comet's codegen dispatcher.

statement
CREATE TABLE test_transform_values(m map<string, int>, delta int) USING parquet

statement
INSERT INTO test_transform_values VALUES
  (map('a', 1, 'b', 2), 10),
  (map('x', -1), 5),
  (map(), 0),
  (NULL, NULL)

-- rewrite values using value
query
SELECT transform_values(m, (k, v) -> v + 1) FROM test_transform_values

-- rewrite values using key and value
query
SELECT transform_values(m, (k, v) -> concat(k, '=', cast(v AS string))) FROM test_transform_values

-- column capture
query
SELECT transform_values(m, (k, v) -> v + delta) FROM test_transform_values

-- all literals
query
SELECT transform_values(map('a', 1, 'b', 2), (k, v) -> v * 100)
