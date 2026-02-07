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

-- Iceberg V3 aggregations and grouping

statement
CREATE NAMESPACE IF NOT EXISTS iceberg_cat.db

statement
CREATE TABLE iceberg_cat.db.v3_orders (order_id INT, customer_id INT, amount DOUBLE, category STRING) USING iceberg TBLPROPERTIES ('format-version' = '3')

statement
INSERT INTO iceberg_cat.db.v3_orders VALUES (1, 1, 100.0, 'A'), (2, 1, 200.0, 'B'), (3, 2, 150.0, 'A'), (4, 3, 300.0, 'B'), (5, 2, 50.0, 'A')

query spark_answer_only
SELECT COUNT(*) FROM iceberg_cat.db.v3_orders

query spark_answer_only
SELECT SUM(amount) FROM iceberg_cat.db.v3_orders

query spark_answer_only
SELECT AVG(amount) FROM iceberg_cat.db.v3_orders

query spark_answer_only
SELECT MIN(amount), MAX(amount) FROM iceberg_cat.db.v3_orders

query spark_answer_only
SELECT customer_id, SUM(amount) as total FROM iceberg_cat.db.v3_orders GROUP BY customer_id ORDER BY customer_id

query spark_answer_only
SELECT category, COUNT(*) as cnt, AVG(amount) as avg_amt FROM iceberg_cat.db.v3_orders GROUP BY category ORDER BY category

query spark_answer_only
SELECT customer_id, SUM(amount) as total FROM iceberg_cat.db.v3_orders GROUP BY customer_id HAVING SUM(amount) > 200 ORDER BY customer_id
