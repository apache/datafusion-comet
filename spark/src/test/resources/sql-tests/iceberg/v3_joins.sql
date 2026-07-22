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

-- Iceberg V3 join operations

statement
CREATE NAMESPACE IF NOT EXISTS iceberg_cat.db

statement
CREATE TABLE iceberg_cat.db.v3_customers (id INT, name STRING) USING iceberg TBLPROPERTIES ('format-version' = '3')

statement
CREATE TABLE iceberg_cat.db.v3_orders_join (order_id INT, customer_id INT, amount DOUBLE) USING iceberg TBLPROPERTIES ('format-version' = '3')

statement
INSERT INTO iceberg_cat.db.v3_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')

statement
INSERT INTO iceberg_cat.db.v3_orders_join VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0), (4, 3, 300.0)

query spark_answer_only
SELECT o.order_id, c.name, o.amount FROM iceberg_cat.db.v3_orders_join o JOIN iceberg_cat.db.v3_customers c ON o.customer_id = c.id ORDER BY o.order_id

query spark_answer_only
SELECT c.name, SUM(o.amount) as total FROM iceberg_cat.db.v3_orders_join o JOIN iceberg_cat.db.v3_customers c ON o.customer_id = c.id GROUP BY c.name ORDER BY c.name

query spark_answer_only
SELECT c.name, COUNT(o.order_id) as order_count FROM iceberg_cat.db.v3_customers c LEFT JOIN iceberg_cat.db.v3_orders_join o ON c.id = o.customer_id GROUP BY c.name ORDER BY c.name
