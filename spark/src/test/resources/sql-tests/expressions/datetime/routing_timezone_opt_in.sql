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
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.comet.expression.ConvertTimezone.allowIncompatible=true

statement
CREATE TABLE routing_timezone(ts TIMESTAMP_NTZ) USING parquet

statement
INSERT INTO routing_timezone VALUES (TIMESTAMP_NTZ '2024-06-15 12:34:56'), (NULL)

query expect_native(convert_timezone)
SELECT convert_timezone('UTC', 'America/Los_Angeles', ts) FROM routing_timezone
