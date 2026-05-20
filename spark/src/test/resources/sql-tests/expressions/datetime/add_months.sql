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

-- ConfigMatrix: parquet.enable.dictionary=false,true

-- Spark's AddMonths expects (DateType, IntegerType) and rolls back the day
-- when the destination month has fewer days (e.g. Jan 31 + 1 month = Feb 28).

statement
CREATE TABLE test_add_months(d date, n int) USING parquet

statement
INSERT INTO test_add_months VALUES
  (date('2024-01-31'), 1),
  (date('2024-01-31'), 12),
  (date('2024-01-31'), -1),
  (date('2024-02-29'), 12),
  (date('2024-02-29'), -12),
  (date('2020-02-29'), 48),
  (date('1970-01-01'), 0),
  (date('1970-01-01'), 2147483647),
  (date('1970-01-01'), -2147483648),
  (date('9999-12-01'), 1),
  (date('0001-01-01'), -1),
  (NULL, 1),
  (date('2024-01-15'), NULL)

query
SELECT add_months(d, n) FROM test_add_months

-- literal arguments
query
SELECT add_months(date('2024-01-31'), 1),
       add_months(date('2024-01-31'), -1),
       add_months(date('2020-02-29'), 12),
       add_months(NULL, 1),
       add_months(date('2024-01-15'), NULL)
