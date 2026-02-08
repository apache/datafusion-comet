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

-- Config: spark.sql.catalog.test_cat=org.apache.iceberg.spark.SparkCatalog
-- Config: spark.sql.catalog.test_cat.type=hadoop
-- Config: spark.sql.catalog.test_cat.warehouse=/tmp/iceberg-warehouse
-- Config: spark.comet.scan.icebergNative.enabled=true

statement
CREATE DATABASE IF NOT EXISTS test_cat.db

statement
CREATE TABLE test_cat.db.test_years_iceberg (
  id INT,
  event_date DATE,
  value STRING
) USING iceberg
PARTITIONED BY (years(event_date))

statement
INSERT INTO test_cat.db.test_years_iceberg VALUES
  (1, DATE '2022-06-15', 'a'),
  (2, DATE '2023-03-20', 'b'),
  (3, DATE '2023-11-10', 'c'),
  (4, DATE '2024-01-05', 'd'),
  (5, DATE '2024-07-20', 'e'),
  (6, DATE '2024-12-31', 'f')

query
SELECT * FROM test_cat.db.test_years_iceberg ORDER BY id

query
SELECT * FROM test_cat.db.test_years_iceberg WHERE event_date = DATE '2023-03-20'

query
SELECT * FROM test_cat.db.test_years_iceberg WHERE event_date >= DATE '2023-01-01' AND event_date < DATE '2024-01-01' ORDER BY id

query
SELECT year(event_date) as yr, COUNT(*) as cnt FROM test_cat.db.test_years_iceberg GROUP BY year(event_date) ORDER BY yr

statement
DROP TABLE test_cat.db.test_years_iceberg
