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

-- Default engine: json_array_length runs Spark's own implementation through the codegen
-- dispatcher, so it stays in the Comet pipeline while matching Spark byte-for-byte, including for
-- inputs the native (rust) path cannot handle compatibly. The native path is opt-in via
-- spark.comet.expression.LengthOfJsonArray.allowIncompatible=true (see json_array_length.sql).

-- Config: spark.comet.expression.LengthOfJsonArray.allowIncompatible=false

-- single-quoted JSON: the native serde_json path is incompatible here, but Spark's lenient parser
-- (run via the dispatcher) accepts it, so the default path matches Spark.
query
SELECT json_array_length("[{'key':'value'}]")

-- trailing content: likewise handled compatibly by the default path.
query
SELECT json_array_length('[1,2,3] trailing')

query
SELECT json_array_length('[1,2,3,4]'), json_array_length('not an array'), json_array_length(NULL)
