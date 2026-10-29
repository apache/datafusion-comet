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

-- Spark evaluates AND, OR and IN lazily, so a guard before a division keeps it from raising on
-- the elements the guard rules out. The native lambda evaluates every operand over every
-- element, so under ANSI mode a lambda that short-circuits must stay on the dispatcher unless
-- nothing in it can raise. None of these queries may raise.

-- Config: spark.sql.ansi.enabled=true
-- ConfigMatrix: spark.comet.exec.nativeLambda.enabled=true,false

statement
CREATE TABLE test_filter_lambda_ansi(id int, ints array<int>, c int) USING parquet

statement
INSERT INTO test_filter_lambda_ansi VALUES
  (1, array(0, 1, NULL, 5, 100), 0),
  (2, array(), 1),
  (3, NULL, 2),
  (4, array(0, 0), 3),
  (5, array(2147483647, 0, -7), 4)

query
SELECT id, filter(ints, x -> x != 0 AND 100 / x > 1) FROM test_filter_lambda_ansi

query
SELECT id, filter(ints, x -> x = 0 OR 100 / x > 1) FROM test_filter_lambda_ansi

query
SELECT id, filter(ints, x -> NOT (x = 0) AND 100 / x > 1) FROM test_filter_lambda_ansi

query
SELECT id, filter(ints, x -> x IN (0, 100 / x)) FROM test_filter_lambda_ansi

query
SELECT id, filter(ints, x -> x IS NOT NULL AND x > 0 AND x < 1000 AND x + 1 > 0 AND 100 / x > 1) FROM test_filter_lambda_ansi

-- CASE, IF and COALESCE are lazy on the native side too.
query
SELECT id, filter(ints, x -> CASE WHEN x = 0 THEN false ELSE 100 / x > 1 END) FROM test_filter_lambda_ansi

query
SELECT id, filter(ints, x -> if(x = 0, false, 100 / x > 1)) FROM test_filter_lambda_ansi

query
SELECT id, filter(ints, x -> coalesce(x, 100 / x) > 1) FROM test_filter_lambda_ansi

-- Nothing here can raise, so these stay native.
query
SELECT id, filter(ints, x -> x IS NOT NULL AND x > c) FROM test_filter_lambda_ansi

query
SELECT id, filter(ints, x -> x IS NULL OR x != 0) FROM test_filter_lambda_ansi
