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
-- Config: spark.comet.expression.MapFromEntries.allowIncompatible=false

statement
CREATE TABLE routing_maps(s STRING, entries ARRAY<STRUCT<key: STRING, value: INT>>, binary_entries ARRAY<STRUCT<key: BINARY, value: INT>>) USING parquet

statement
INSERT INTO routing_maps VALUES ('a:1,b:2', array(named_struct('key', 'a', 'value', 1)), array(named_struct('key', unhex('41'), 'value', 1))), ('', array(), array()), (NULL, NULL, NULL)

query expect_native(str_to_map)
SELECT str_to_map(s) FROM routing_maps

query expect_native(map_from_entries)
SELECT map_from_entries(entries) FROM routing_maps

query expect_dispatch(map_from_entries)
SELECT map_from_entries(binary_entries) FROM routing_maps
