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
-- Config: spark.comet.expression.MakeInterval.allowIncompatible=true

statement
CREATE TABLE routing_intervals(i INT) USING parquet

statement
INSERT INTO routing_intervals VALUES (-2), (0), (NULL)

query expect_native(abs)
SELECT abs(i) FROM routing_intervals

query expect_fallback(abs: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT abs(make_dt_interval(i)), abs(make_ym_interval(i)) FROM routing_intervals

query expect_native(make_interval)
SELECT make_interval(i, 0, 0, 0, 0, 0, 0) FROM routing_intervals
