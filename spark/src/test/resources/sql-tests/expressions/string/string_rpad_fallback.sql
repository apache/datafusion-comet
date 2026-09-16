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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE test_rpad_fallback(s string, len int, pad string) USING parquet

statement
INSERT INTO test_rpad_fallback VALUES ('hi', 5, 'xy'), ('hello', 3, 'x'), ('', 3, 'a'), ('', 0, 'x'), ('hi', 5, ''), (NULL, 5, 'x'), ('hi', NULL, 'x'), ('hi', 5, NULL), (NULL, NULL, NULL)

query expect_fallback(rpad: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT rpad(s, len, pad) FROM test_rpad_fallback

query expect_fallback(rpad: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT rpad('hi', len, 'xy') FROM test_rpad_fallback

query expect_fallback(rpad: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT rpad('hi', 5, 'xy')

query expect_fallback(rpad: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT rpad('hi', 5, 'xy') FROM test_rpad_fallback

-- The native argument shapes do not require the dispatcher.
query expect_native(rpad)
SELECT rpad(s, len, 'xy') FROM test_rpad_fallback

query expect_native(rpad)
SELECT rpad(s, len) FROM test_rpad_fallback
