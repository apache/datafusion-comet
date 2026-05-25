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

statement
CREATE TABLE test_add_months(d date, m int) USING parquet

statement
INSERT INTO test_add_months VALUES
  (date('2024-01-15'),  1),
  (date('2024-12-15'),  2),
  (date('2024-03-15'), -2),
  (date('2024-06-15'),  0),
  (date('2024-01-31'),  1),
  (date('2023-01-31'),  1),
  (date('2024-02-29'), 12),
  (date('2024-01-15'),  24),
  (date('2024-01-15'), -24),
  (date('1970-01-01'),  600),
  (date('2024-06-15'),  NULL),
  (NULL,                1),
  (NULL,                NULL)

query
SELECT d, m, add_months(d, m) FROM test_add_months
