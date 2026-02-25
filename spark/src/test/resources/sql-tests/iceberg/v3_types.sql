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

-- Iceberg V3 supported data types

statement
CREATE NAMESPACE IF NOT EXISTS iceberg_cat.db

statement
CREATE TABLE iceberg_cat.db.v3_types (
  id INT,
  big_num BIGINT,
  price DECIMAL(10, 2),
  event_date DATE,
  event_time TIMESTAMP,
  is_active BOOLEAN,
  description STRING,
  score FLOAT,
  rating DOUBLE
) USING iceberg TBLPROPERTIES ('format-version' = '3')

statement
INSERT INTO iceberg_cat.db.v3_types VALUES (1, 9223372036854775807, 123.45, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00', true, 'Test', 3.14, 2.718)

query spark_answer_only
SELECT * FROM iceberg_cat.db.v3_types

query spark_answer_only
SELECT id, big_num FROM iceberg_cat.db.v3_types WHERE big_num > 1000

query spark_answer_only
SELECT id, price FROM iceberg_cat.db.v3_types WHERE price > 100.00

query spark_answer_only
SELECT id, event_date FROM iceberg_cat.db.v3_types WHERE event_date = DATE '2024-01-15'

query spark_answer_only
SELECT id, is_active FROM iceberg_cat.db.v3_types WHERE is_active = true
