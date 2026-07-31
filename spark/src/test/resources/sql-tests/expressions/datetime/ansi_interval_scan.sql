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

-- Config: spark.sql.sources.useV1SourceList=parquet

statement
CREATE TABLE test_ansi_interval_scan(
  id INT,
  ym INTERVAL YEAR TO MONTH,
  dt INTERVAL DAY TO SECOND)
USING parquet

statement
INSERT INTO test_ansi_interval_scan VALUES
  (1, INTERVAL '1-2' YEAR TO MONTH, INTERVAL '1 02:03:04.5' DAY TO SECOND),
  (2, INTERVAL '-2-3' YEAR TO MONTH, INTERVAL '-2 03:04:05.6' DAY TO SECOND),
  (3, NULL, NULL)

query
SELECT id, ym, dt FROM test_ansi_interval_scan
