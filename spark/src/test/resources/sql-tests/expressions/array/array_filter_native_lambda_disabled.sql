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

-- With the native lambda off, every lambda runs on the dispatcher.

-- Config: spark.comet.exec.nativeLambda.enabled=false

statement
CREATE TABLE test_filter_lambda_off(id int, strs array<string>, ints array<int>, c int) USING parquet

statement
INSERT INTO test_filter_lambda_off VALUES (1, array('a', NULL, '', 'b'), array(1, NULL, 3), 2), (2, array(), array(), 1), (3, NULL, NULL, 0)

query expect_dispatch(filter)
SELECT id, filter(strs, x -> nvl(x, '') != '') FROM test_filter_lambda_off

query expect_dispatch(filter)
SELECT id, filter(ints, x -> x > c) FROM test_filter_lambda_off
