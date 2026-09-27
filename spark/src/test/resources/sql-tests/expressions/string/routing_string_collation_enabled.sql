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
-- Config: spark.comet.expression.Concat.allowIncompatible=false
-- Config: spark.comet.expression.Reverse.allowIncompatible=false

statement
CREATE TABLE routing_string_collation(s STRING, plain STRING) USING parquet

statement
INSERT INTO routing_string_collation VALUES ('Hello', 'hello'), ('', ''), (NULL, NULL)

query expect_native(reverse)
SELECT reverse(plain) FROM routing_string_collation

query expect_native(levenshtein)
SELECT levenshtein(plain, 'hello') FROM routing_string_collation

query expect_dispatch(concat)
SELECT concat(s COLLATE UTF8_LCASE, '!') FROM routing_string_collation

query expect_dispatch(reverse)
SELECT reverse(s COLLATE UTF8_LCASE) FROM routing_string_collation

query expect_dispatch(like)
SELECT (s COLLATE UTF8_LCASE) LIKE 'H_llo' FROM routing_string_collation

query expect_dispatch(levenshtein)
SELECT levenshtein(s COLLATE UTF8_LCASE, 'HELLO') FROM routing_string_collation
