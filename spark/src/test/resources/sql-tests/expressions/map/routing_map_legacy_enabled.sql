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
-- Config: spark.sql.legacy.truncateForEmptyRegexSplit=true
-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN
-- Config: spark.comet.expression.StringToMap.allowIncompatible=false
-- Config: spark.comet.expression.MapFromEntries.allowIncompatible=false

statement
CREATE TABLE routing_map_legacy(s STRING, e ARRAY<STRUCT<key: STRING, value: INT>>) USING parquet

statement
INSERT INTO routing_map_legacy VALUES ('a:1,b:2', array(named_struct('key', 'a', 'value', 1))), (NULL, NULL)

query expect_dispatch(str_to_map)
SELECT str_to_map(s) FROM routing_map_legacy

-- `MapFromEntries` no longer declines under `LAST_WIN`: the native builder reads the policy from
-- `datafusion.spark.map_key_dedup_policy`, so it stays native whatever the codegen flag says. Its
-- dispatch and fallback routes are still covered by the `BinaryType` queries in
-- `routing_maps_enabled.sql` and `routing_maps_disabled.sql`.
query expect_native(map_from_entries)
SELECT map_from_entries(e) FROM routing_map_legacy
