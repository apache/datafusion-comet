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

-- dayname (Spark 4.0+) is implemented natively. It maps a date to a fixed US-English abbreviated
-- day name (DayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US)), with no session-locale or
-- timezone dependence.
-- MinSparkVersion: 4.0

statement
CREATE TABLE test_dayname(d date) USING parquet

statement
INSERT INTO test_dayname VALUES
  (date('2024-01-15')),
  (date('2024-06-30')),
  (date('2020-12-31')),
  (date('1970-01-01')),
  (NULL)

query
SELECT dayname(d) FROM test_dayname

-- literal argument
query
SELECT dayname(date('2024-02-29'))
