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
-- Config: spark.comet.expression.RLike.allowIncompatible=false
-- Config: spark.comet.expression.RegExpExtract.allowIncompatible=false
-- Config: spark.comet.expression.RegExpReplace.allowIncompatible=false
-- Config: spark.comet.expression.StringSplit.allowIncompatible=false

statement
CREATE TABLE routing_regex(s STRING, p STRING, i INT) USING parquet

statement
INSERT INTO routing_regex VALUES ('ab12ab', '(ab)', 1), ('', '(ab)', 1), (NULL, '(ab)', 1)

query expect_native(rlike)
SELECT s RLIKE '(ab)' FROM routing_regex

query expect_fallback(rlike: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT s RLIKE p FROM routing_regex

query expect_fallback(regexp_extract: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT regexp_extract(s, '(ab)', 1) FROM routing_regex

query expect_fallback(regexp_extract: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT regexp_extract(s, p, i) FROM routing_regex

query expect_fallback(regexp_replace: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT regexp_replace(s, 'ab', 'x') FROM routing_regex

query expect_fallback(regexp_replace: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT regexp_replace(s, 'ab', 'x', 2) FROM routing_regex

query expect_fallback(split: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT split(s, 'ab') FROM routing_regex
