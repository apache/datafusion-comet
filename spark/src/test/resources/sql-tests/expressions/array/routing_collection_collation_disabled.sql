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
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.comet.expression.ArrayJoin.allowIncompatible=false
-- Config: spark.comet.expression.MapFromEntries.allowIncompatible=false
-- Config: spark.comet.expression.StringToMap.allowIncompatible=false

statement
CREATE TABLE routing_collection_collation(s STRING, a ARRAY<STRING>) USING parquet

statement
INSERT INTO routing_collection_collation VALUES ('a:1,b:2', array('a', 'B')), ('', array()), (NULL, NULL)

query expect_fallback(array_join: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT array_join(transform(a, x -> x COLLATE UTF8_LCASE), ',') FROM routing_collection_collation

query expect_fallback(str_to_map: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT str_to_map(s COLLATE UTF8_LCASE) FROM routing_collection_collation

query expect_fallback(map_from_entries: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT map_from_entries(transform(a, x -> named_struct('key', x COLLATE UTF8_LCASE, 'value', 1)))
FROM routing_collection_collation
