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
CREATE TABLE test_lpad_fallback(s string, len int, pad string) USING parquet

statement
INSERT INTO test_lpad_fallback VALUES ('hi', 5, 'xy'), ('hello', 3, 'x'), (NULL, NULL, NULL)

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT lpad(s, len, pad) FROM test_lpad_fallback

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT lpad('hi', len, 'xy') FROM test_lpad_fallback

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT lpad('hi', 5, 'xy')

-- The native argument shapes do not require the dispatcher.
query
SELECT lpad(s, len, 'xy') FROM test_lpad_fallback

query
SELECT lpad(s, len) FROM test_lpad_fallback
