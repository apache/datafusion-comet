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
-- Config: spark.comet.expression.InitCap.allowIncompatible=false
-- Config: spark.comet.expression.StringReplace.allowIncompatible=false
-- Config: spark.comet.expression.GetJsonObject.allowIncompatible=false

statement
CREATE TABLE routing_string_opt_in(s STRING, j STRING) USING parquet

statement
INSERT INTO routing_string_opt_in VALUES ('hello world', '{"a":1}'), ('', '{}'), (NULL, NULL)

query expect_dispatch(initcap)
SELECT initcap(s) FROM routing_string_opt_in

query expect_dispatch(replace)
SELECT replace(s, 'l', 'L') FROM routing_string_opt_in

query expect_dispatch(get_json_object)
SELECT get_json_object(j, '$.a') FROM routing_string_opt_in
