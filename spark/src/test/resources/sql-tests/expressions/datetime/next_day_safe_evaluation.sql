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

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE next_day_safe(d DATE, dow STRING) USING parquet

statement
INSERT INTO next_day_safe VALUES (DATE '2026-09-01', 'Monday'), (NULL, 'Tuesday')

-- A valid literal cannot raise an invalid-weekday error, including below LIMIT.
query expect_native(next_day)
SELECT next_day(d, 'Monday') FROM next_day_safe LIMIT 1

-- Spark evaluates the date operand and these eager inputs for every consumed row.
query expect_native(next_day)
SELECT date_add(next_day(d, dow), 1), datediff(next_day(d, dow), d),
       concat(CAST(next_day(d, dow) AS STRING), 'x'), greatest(next_day(d, dow), d)
FROM next_day_safe

query expect_native(next_day)
SELECT d FROM next_day_safe ORDER BY next_day(d, dow)

query expect_native(next_day)
SELECT max(next_day(d, dow)) FROM next_day_safe
