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

-- Iceberg V3 basic table operations

statement
CREATE NAMESPACE IF NOT EXISTS iceberg_cat.db

statement
CREATE TABLE iceberg_cat.db.v3_basic (id INT, name STRING, value DOUBLE) USING iceberg TBLPROPERTIES ('format-version' = '3')

statement
INSERT INTO iceberg_cat.db.v3_basic VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.7)

query spark_answer_only
SELECT * FROM iceberg_cat.db.v3_basic ORDER BY id

query spark_answer_only
SELECT * FROM iceberg_cat.db.v3_basic WHERE id > 1 ORDER BY id

query spark_answer_only
SELECT * FROM iceberg_cat.db.v3_basic WHERE name = 'Alice'

query spark_answer_only
SELECT COUNT(*) FROM iceberg_cat.db.v3_basic

query spark_answer_only
SELECT id, name FROM iceberg_cat.db.v3_basic WHERE value > 15.0 ORDER BY id
