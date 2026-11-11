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

-- MinSparkVersion: 4.0
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.optimizer.inSetConversionThreshold=0

statement
CREATE TABLE routing_predicates(s STRING, t STRING, a STRING, b STRING) USING parquet

statement
INSERT INTO routing_predicates VALUES ('Hello', 'HELLO', 'Hello', 'HELLO'), ('', '', '', ''), (NULL, NULL, NULL, NULL)

query expect_native(equalto)
SELECT a = b FROM routing_predicates

query expect_native(equalnullsafe)
SELECT a <=> b FROM routing_predicates

query expect_native(lessthan)
SELECT a < b FROM routing_predicates

query expect_native(lessthanorequal)
SELECT a <= b FROM routing_predicates

query expect_native(greaterthan)
SELECT a > b FROM routing_predicates

query expect_native(greaterthanorequal)
SELECT a >= b FROM routing_predicates

query expect_native(contains)
SELECT contains(a, b) FROM routing_predicates

query expect_native(startswith)
SELECT startswith(a, b) FROM routing_predicates

query expect_native(endswith)
SELECT endswith(a, b) FROM routing_predicates

query expect_native(in)
SELECT a IN (b, 'other') FROM routing_predicates

query expect_native(inset)
SELECT a IN ('HELLO', 'a', 'b') FROM routing_predicates

query expect_dispatch(equalto)
SELECT (s COLLATE UTF8_LCASE) = (t COLLATE UTF8_LCASE) FROM routing_predicates

query expect_dispatch(equalnullsafe)
SELECT (s COLLATE UTF8_LCASE) <=> (t COLLATE UTF8_LCASE) FROM routing_predicates

query expect_dispatch(lessthan)
SELECT (s COLLATE UTF8_LCASE) < (t COLLATE UTF8_LCASE) FROM routing_predicates

query expect_dispatch(lessthanorequal)
SELECT (s COLLATE UTF8_LCASE) <= (t COLLATE UTF8_LCASE) FROM routing_predicates

query expect_dispatch(greaterthan)
SELECT (s COLLATE UTF8_LCASE) > (t COLLATE UTF8_LCASE) FROM routing_predicates

query expect_dispatch(greaterthanorequal)
SELECT (s COLLATE UTF8_LCASE) >= (t COLLATE UTF8_LCASE) FROM routing_predicates

query expect_dispatch(contains)
SELECT contains((s COLLATE UTF8_LCASE), (t COLLATE UTF8_LCASE)) FROM routing_predicates

query expect_dispatch(startswith)
SELECT startswith((s COLLATE UTF8_LCASE), (t COLLATE UTF8_LCASE)) FROM routing_predicates

query expect_dispatch(endswith)
SELECT endswith((s COLLATE UTF8_LCASE), (t COLLATE UTF8_LCASE)) FROM routing_predicates

query expect_dispatch(in)
SELECT (s COLLATE UTF8_LCASE) IN ((t COLLATE UTF8_LCASE), 'other') FROM routing_predicates

query expect_dispatch(inset)
SELECT (s COLLATE UTF8_LCASE) IN ('HELLO', 'a', 'b') FROM routing_predicates
