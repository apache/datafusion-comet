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
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.comet.expression.StructsToCsv.allowIncompatible=false

statement
CREATE TABLE routing_csv(s STRING, i INT, a ARRAY<INT>) USING parquet

statement
INSERT INTO routing_csv VALUES ('abc', 1, array(1, 2)), ('', 0, array()), (NULL, NULL, NULL)

query expect_fallback(to_csv: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT to_csv(named_struct('s', s, 'i', i)) FROM routing_csv

-- Keep the complex field non-null so Spark can produce the baseline.
query expect_fallback(to_csv: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT to_csv(named_struct('a', a)) FROM routing_csv WHERE a IS NOT NULL
