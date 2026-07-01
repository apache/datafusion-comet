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

-- Europe/London observes DST, so NTZ values that land on the spring-forward gap and the
-- fall-back overlap exercise the timezone-sensitive NTZ -> Timestamp cast. The native path
-- must resolve these the same way Spark does.
-- Config: spark.sql.session.timeZone=Europe/London

statement
CREATE TABLE test_ntz_dst(ts_ntz timestamp_ntz) USING parquet

statement
INSERT INTO test_ntz_dst VALUES
  (cast('2024-03-31 01:30:00' as timestamp_ntz)),
  (cast('2024-10-27 01:30:00' as timestamp_ntz))

query
SELECT ts_ntz, cast(ts_ntz as timestamp) FROM test_ntz_dst ORDER BY ts_ntz
