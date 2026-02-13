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

-- migrated from CometExpressionSuite "test concat function - arrays"
-- https://github.com/apache/datafusion-comet/issues/2647

statement
CREATE TABLE test_array_concat(c1 array<int>, c2 array<int>, c3 array<int>, c4 array<int>, c5 array<int>) USING parquet

statement
INSERT INTO test_array_concat VALUES (array(0, 1), array(2, 3), array(), array(null), null), (array(1, 2), array(3, 4), array(), array(null), null), (array(2, 3), array(4, 5), array(), array(null), null)

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(c1, c2) AS x FROM test_array_concat

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(c1, c1) AS x FROM test_array_concat

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(c1, c2, c3) AS x FROM test_array_concat

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(c1, c2, c3, c5) AS x FROM test_array_concat

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(concat(c1, c2, c3), concat(c1, c3)) AS x FROM test_array_concat
