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

-- Spark's `exists` stops at the first element that matches, so it never evaluates the lambda on the
-- elements after it. The native lambda evaluates every element, so under ANSI mode a lambda in which any
-- expression can raise stays on the dispatcher, whatever operators it uses. In every row below an
-- element that matches comes before any element on which the lambda raises, so none of these queries
-- may raise.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.legacy.followThreeValuedLogicInArrayExists=true

statement
CREATE TABLE test_exists_lambda_ansi(id int, ints array<int>, strs array<string>) USING parquet

statement
INSERT INTO test_exists_lambda_ansi VALUES
  (1, array(1, 0), array('5', 'a')),
  (2, array(), array()),
  (3, NULL, NULL),
  (4, array(NULL, 5, 0), array(NULL, '7', '')),
  (5, array(5, 2147483647), array('9', '2147483648')),
  (6, array(NULL, 1000), array(NULL, '0'))

query expect_dispatch(exists)
SELECT id, exists(ints, x -> 100 / x > 1) FROM test_exists_lambda_ansi

query expect_dispatch(exists)
SELECT id, exists(ints, x -> x + 1 > 1) FROM test_exists_lambda_ansi

query expect_dispatch(exists)
SELECT id, exists(strs, x -> CAST(x AS INT) > 1) FROM test_exists_lambda_ansi

-- CASE is lazy natively too, but that does not help when the lambda raises on an element after the match.
query expect_dispatch(exists)
SELECT id, exists(ints, x -> CASE WHEN x IS NULL THEN false ELSE 10 / x > 1 END) FROM test_exists_lambda_ansi

-- Nothing here can raise, so these stay native.
query expect_native(exists)
SELECT id, exists(ints, x -> x > 1) FROM test_exists_lambda_ansi

query expect_native(exists)
SELECT id, exists(strs, x -> x IS NULL OR x = '') FROM test_exists_lambda_ansi
