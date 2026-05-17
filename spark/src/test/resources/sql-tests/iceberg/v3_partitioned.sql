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

-- Iceberg V3 partitioned tables

statement
CREATE NAMESPACE IF NOT EXISTS iceberg_cat.db

statement
CREATE TABLE iceberg_cat.db.v3_partitioned (id INT, event_date DATE, value STRING) USING iceberg PARTITIONED BY (days(event_date)) TBLPROPERTIES ('format-version' = '3')

statement
INSERT INTO iceberg_cat.db.v3_partitioned VALUES (1, DATE '2024-01-01', 'a'), (2, DATE '2024-01-02', 'b'), (3, DATE '2024-01-01', 'c'), (4, DATE '2024-02-01', 'd')

query spark_answer_only
SELECT * FROM iceberg_cat.db.v3_partitioned ORDER BY id

query spark_answer_only
SELECT * FROM iceberg_cat.db.v3_partitioned WHERE event_date = DATE '2024-01-01' ORDER BY id

query spark_answer_only
SELECT * FROM iceberg_cat.db.v3_partitioned WHERE event_date >= DATE '2024-01-02' ORDER BY id

query spark_answer_only
SELECT event_date, COUNT(*) FROM iceberg_cat.db.v3_partitioned GROUP BY event_date ORDER BY event_date
