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

-- convert_timezone returns TIMESTAMP_NTZ, so its Arrow result must have no timezone label.
-- Use an NTZ comparison column to avoid an implicit cast that could repair the label.
-- MinSparkVersion: 3.5
-- ConfigMatrix: spark.sql.session.timeZone=UTC,Etc/UTC,America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.expression.ConvertTimezone.allowIncompatible=true

statement
CREATE TABLE convert_timestamp_labels(ts TIMESTAMP_NTZ, other TIMESTAMP_NTZ) USING parquet

statement
INSERT INTO convert_timestamp_labels VALUES
  (TIMESTAMP_NTZ '1970-01-01 00:00:00', TIMESTAMP_NTZ '1969-12-31 16:00:00'),
  (TIMESTAMP_NTZ '1969-12-31 23:59:59.5', TIMESTAMP_NTZ '1970-01-01 00:00:00'),
  (TIMESTAMP_NTZ '2024-01-15 18:00:00.25', TIMESTAMP_NTZ '2024-01-15 10:00:00.25'),
  (TIMESTAMP_NTZ '2024-06-30 23:30:00.5', TIMESTAMP_NTZ '2024-06-30 16:30:00.5'),
  (TIMESTAMP_NTZ '2024-03-10 09:30:00', TIMESTAMP_NTZ '2024-03-10 01:30:00'),
  (TIMESTAMP_NTZ '2024-03-10 10:30:00', TIMESTAMP_NTZ '2024-03-10 03:30:00'),
  (TIMESTAMP_NTZ '2024-11-03 08:30:00', TIMESTAMP_NTZ '2024-11-03 01:30:00'),
  (TIMESTAMP_NTZ '2024-11-03 09:30:00', TIMESTAMP_NTZ '2024-11-03 01:30:00'),
  (NULL, NULL)

-- An explicit source timezone is independent of the session timezone.
query expect_native(convert_timezone, equalto, lessthan, hour, cast)
SELECT
  convert_timezone('UTC', 'America/Los_Angeles', ts) = other,
  convert_timezone('UTC', 'America/Los_Angeles', ts) < other,
  hour(convert_timezone('UTC', 'America/Los_Angeles', ts)),
  CAST(convert_timezone('UTC', 'America/Los_Angeles', ts) AS STRING)
FROM convert_timestamp_labels
