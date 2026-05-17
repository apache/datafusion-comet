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

-- from_unixtime() under LEGACY timeParserPolicy.
-- Legacy SimpleDateFormat accepts patterns the new java.time formatter rejects.
-- Config: spark.sql.legacy.timeParserPolicy=LEGACY
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_from_unix_time_legacy(t long) USING parquet

statement
INSERT INTO test_from_unix_time_legacy VALUES (0), (1718451045), (NULL)

-- Legacy-only token: 4-char am/pm marker formats successfully under LEGACY.
query spark_answer_only
SELECT from_unixtime(t, 'yyyy-MM-dd aaaa') FROM test_from_unix_time_legacy
