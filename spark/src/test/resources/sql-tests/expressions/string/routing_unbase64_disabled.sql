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
CREATE TABLE routing_unbase64(s STRING) USING parquet

statement
INSERT INTO routing_unbase64 VALUES ('aGVsbG8='), (''), (NULL)

query expect_native(unbase64)
SELECT unbase64(s) FROM routing_unbase64

query expect_fallback(unbase64: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT unbase64(concat(s, '')) FROM routing_unbase64

query expect_fallback(unbase64: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT to_binary(s, 'base64') FROM routing_unbase64
