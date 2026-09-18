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

statement
CREATE TABLE test_lpad(s string, len int, pad string) USING parquet

statement
INSERT INTO test_lpad VALUES ('hi', 5, 'x'), ('hello', 3, 'x'), ('hi', 5, 'xy'), ('', 3, 'a'), ('', 0, 'x'), (NULL, 5, 'x'), ('hi', 0, 'x'), ('hi', -1, 'x'), ('hi', NULL, 'x'), (NULL, NULL, 'x'), ('hi', 5, NULL), ('hi', 5, ''), ('', 3, ''), ('hi', -100, 'x'), (NULL, NULL, NULL), ('"hi', 7, '"x'), ('café', 7, '中文'), ('é', 5, '🙂')

-- Column padding runs through the codegen dispatcher (issue #5579).
query expect_dispatch(lpad)
SELECT lpad(s, len, pad) FROM test_lpad

query expect_dispatch(lpad)
SELECT lpad(s, 5, pad) FROM test_lpad

query expect_dispatch(lpad)
SELECT lpad('hi', len, pad) FROM test_lpad

query expect_dispatch(lpad)
SELECT lpad('hi', len, 'xy') FROM test_lpad

query expect_dispatch(lpad)
SELECT lpad('hi', len) FROM test_lpad

query expect_native(lpad)
SELECT lpad(s, len) FROM test_lpad

-- column + column + literal
query expect_native(lpad)
SELECT lpad(s, len, 'x') FROM test_lpad

query expect_native(lpad)
SELECT lpad(s, len, 'xy') FROM test_lpad

-- column + literal + literal
query expect_native(lpad)
SELECT lpad(s, 5, 'x') FROM test_lpad

-- literal + literal + literal
query expect_dispatch(lpad)
SELECT lpad('hi', 5, 'x'), lpad('hello', 3, 'x'), lpad('', 3, 'a'), lpad(NULL, 5, 'x')

query expect_dispatch(lpad)
SELECT lpad('hi', 5, 'xy') FROM test_lpad
