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

-- MinSparkVersion: 3.5
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.comet.expression.RegExpExtractAll.allowIncompatible=true

statement
CREATE TABLE routing_regex_all(s STRING, p STRING, i INT) USING parquet

statement
INSERT INTO routing_regex_all VALUES ('ab12ab', '(ab)', 1), ('', '(ab)', 1), (NULL, '(ab)', 1)

query expect_native(regexp_extract_all)
SELECT regexp_extract_all(s, '(ab)', 1) FROM routing_regex_all

query expect_fallback(regexp_extract_all: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT regexp_extract_all(s, p, i) FROM routing_regex_all
