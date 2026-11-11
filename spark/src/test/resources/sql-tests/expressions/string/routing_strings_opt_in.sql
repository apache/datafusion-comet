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
-- Config: spark.comet.expression.StringTranslate.allowIncompatible=true

statement
CREATE TABLE routing_strings(s STRING, p STRING, b BINARY, a ARRAY<INT>) USING parquet

statement
INSERT INTO routing_strings VALUES ('h_llo', 'h_llo', unhex('4142'), array(1, 2)), ('hello', 'h_llo', unhex(''), array()), (NULL, NULL, NULL, NULL)

query expect_native(like)
SELECT s LIKE p FROM routing_strings

query expect_native(concat)
SELECT concat(s, '!') FROM routing_strings

query expect_native(concat_ws)
SELECT concat_ws('-', s, 'x') FROM routing_strings

query expect_fallback(like: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT s LIKE 'h$_llo' ESCAPE '$' FROM routing_strings

query expect_fallback(concat: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT concat(b, b), concat(a, a) FROM routing_strings

query expect_fallback(concat_ws: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT concat_ws('-', 'a', 'b')

query expect_native(translate)
SELECT translate(s, 'hl', 'HL') FROM routing_strings
