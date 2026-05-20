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

-- The result is milliseconds since epoch in UTC, so it must not depend on the
-- session timezone.
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

statement
CREATE TABLE test_unix_millis(ts timestamp) USING parquet

statement
INSERT INTO test_unix_millis VALUES
  (timestamp('1970-01-01 00:00:00')),
  (timestamp('2024-01-15 12:34:56.123456')),
  (timestamp('1969-12-31 23:59:59.999999')),
  (timestamp('9999-12-31 23:59:59.999999')),
  (timestamp('0001-01-01 00:00:00')),
  (NULL)

query
SELECT unix_millis(ts) FROM test_unix_millis

-- literal arguments
query
SELECT unix_millis(timestamp('1970-01-01 00:00:00')),
       unix_millis(timestamp('2024-01-15 12:34:56.123456')),
       unix_millis(NULL)
