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

-- Config: spark.sql.datetime.java8API.enabled=true
-- Config: spark.sql.ansi.enabled=true
-- ConfigMatrix: spark.sql.session.timeZone=Europe/Amsterdam,Pacific/Apia,Australia/Sydney

statement
CREATE TABLE region_dates(y INT, m INT, d INT) USING parquet

-- Cover a historical offset with seconds, the dates around Apia's skipped day,
-- and summer and winter in Sydney after the end of chrono-tz's transition table.
statement
INSERT INTO region_dates VALUES
  (1850, 1, 1), (2011, 12, 29), (2011, 12, 30), (2011, 12, 31),
  (2101, 1, 15), (2101, 7, 15)

query expect_native(cast, make_date)
SELECT y, m, d, CAST(make_date(y, m, d) AS TIMESTAMP) FROM region_dates
