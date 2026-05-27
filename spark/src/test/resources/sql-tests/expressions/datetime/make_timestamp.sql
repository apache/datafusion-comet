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

-- Routes make_timestamp through the codegen dispatcher.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_make_timestamp(y int, mo int, d int, h int, mi int, s decimal(8,6)) USING parquet

statement
INSERT INTO test_make_timestamp VALUES
  (2024, 6, 15, 10, 30, 45.123456),
  (1970, 1, 1, 0, 0, 0.0),
  (2024, 12, 31, 23, 59, 59.999999),
  (NULL, 6, 15, 10, 30, 45.0),
  (2024, NULL, 15, 10, 30, 45.0)

query
SELECT make_timestamp(y, mo, d, h, mi, s) FROM test_make_timestamp

-- Explicit timezone argument
query
SELECT make_timestamp(y, mo, d, h, mi, s, 'UTC') FROM test_make_timestamp

-- literal arguments
query
SELECT make_timestamp(2024, 6, 15, 10, 30, 45.000)
