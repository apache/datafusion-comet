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

-- Disabling the dispatcher keeps supported inputs native and sends floating-point inputs to Spark.
statement
CREATE TABLE test_round_disabled(d double, f float, dec decimal(10,4), i int, l bigint) USING parquet

statement
INSERT INTO test_round_disabled VALUES
 (2.5, 2.5, 2.5, 25, 25),
 (-2.5, -2.5, -2.5, -25, -25),
 (NULL, NULL, NULL, NULL, NULL)

query expect_native(round)
SELECT round(dec, 2), round(i, -1), round(l, -1) FROM test_round_disabled

query expect_fallback(round: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT round(d, 2) FROM test_round_disabled

query expect_fallback(round: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT round(f, 2) FROM test_round_disabled
