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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

query
SELECT filter(array(1, 2), (x, i) -> i < 3)

statement
CREATE TABLE test_dispatch(a array<int>, b array<int>, c array<string>) USING parquet;

statement
INSERT INTO test_dispatch VALUES (array(1,2,3), array(10,20,30), array('abc','xyz','a1'));

query
SELECT filter(c, x -> x rlike '^a') FROM test_dispatch;

query
SELECT filter(a, x -> exists(b, y -> y > x)) FROM test_dispatch;

query
SELECT filter(a, x -> array_max(transform(b, y -> y + x)) > 31) FROM test_dispatch;
