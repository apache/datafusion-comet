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

statement
CREATE TABLE test_concat(a string, b string, c string, d string) USING parquet

statement
INSERT INTO test_concat VALUES ('hello', ' ', 'world', NULL), ('', '', '', NULL), (NULL, 'b', 'c', NULL), ('a', NULL, 'c', NULL), (NULL, NULL, NULL, NULL)

query
SELECT concat(a, b, c) FROM test_concat

query
SELECT a || b || c FROM test_concat

-- mixed: column + literal + column
query
SELECT concat(a, ' ', c) FROM test_concat

-- migrated from CometExpressionSuite "test concat function - strings"
-- two arguments
query
SELECT concat(a, b) FROM test_concat

-- same column twice
query
SELECT concat(a, a) FROM test_concat

-- four arguments with null column
query
SELECT concat(a, b, c, d) FROM test_concat

-- nested concat
query
SELECT concat(concat(a, b, c), concat(a, c)) FROM test_concat

-- literal + literal + literal
query
SELECT concat('hello', ' ', 'world'), concat('', '', ''), concat(NULL, 'b', 'c')

-- migrated from CometExpressionSuite "test concat function - binary"
-- https://github.com/apache/datafusion-comet/issues/2647
statement
CREATE TABLE test_concat_binary USING parquet AS SELECT cast(uuid() as binary) c1, cast(uuid() as binary) c2, cast(uuid() as binary) c3, cast(uuid() as binary) c4, cast(null as binary) c5 FROM range(10)

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(c1, c2) AS x FROM test_concat_binary

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(c1, c1) AS x FROM test_concat_binary

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(c1, c2, c3) AS x FROM test_concat_binary

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(c1, c2, c3, c5) AS x FROM test_concat_binary

query expect_fallback(CONCAT supports only string input parameters)
SELECT concat(concat(c1, c2, c3), concat(c1, c3)) AS x FROM test_concat_binary
