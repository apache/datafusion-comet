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

-- Config: spark.sql.session.timeZone=UTC
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_timestamp_seconds(c0 bigint) USING parquet

statement
INSERT INTO test_timestamp_seconds VALUES (0), (1640995200), (-86400), (4102444800), (-2208988800), (NULL)

-- column argument
query
SELECT c0, timestamp_seconds(c0) FROM test_timestamp_seconds

-- literal arguments
query
SELECT timestamp_seconds(0)

query
SELECT timestamp_seconds(1640995200)

-- negative value (before epoch)
query
SELECT timestamp_seconds(-86400)

-- decimal seconds (fractional)
query
SELECT timestamp_seconds(CAST(1640995200.123 AS DOUBLE))

-- null handling
query
SELECT timestamp_seconds(NULL)
