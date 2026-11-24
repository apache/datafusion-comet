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

-- ConfigMatrix: spark.comet.batchSize=3,16,128
-- Config: spark.sql.shuffle.partitions=1

-- The seven nullable key types describe the same groups: 33 NULL rows followed
-- by groups of 17 rows. Groups cross every configured input batch size.
statement
CREATE TABLE streaming_row_number_keys(
  id BIGINT,
  k_int INT,
  k_bigint BIGINT,
  k_decimal DECIMAL(12, 2),
  k_date DATE,
  k_timestamp TIMESTAMP,
  k_string STRING,
  k_binary BINARY,
  payload STRING
) USING parquet

statement
INSERT INTO streaming_row_number_keys
SELECT id,
       g,
       CAST(g AS BIGINT),
       CAST(g AS DECIMAL(12, 2)),
       DATE_ADD(DATE '2000-01-01', g),
       CAST(DATE_ADD(DATE '2000-01-01', g) AS TIMESTAMP),
       CONCAT('key-', CAST(g AS STRING)),
       CAST(CONCAT('key-', CAST(g AS STRING)) AS BINARY),
       CONCAT('payload-', CAST(id AS STRING))
FROM (
  SELECT id,
         CASE WHEN id < 33 THEN CAST(NULL AS INT)
              ELSE CAST((id - 33) DIV 17 AS INT) END AS g
  FROM range(385)
) generated_keys

query
SELECT id, k_int,
       ROW_NUMBER() OVER (PARTITION BY k_int ORDER BY id) AS rn
FROM streaming_row_number_keys
ORDER BY id

query
SELECT id, k_bigint,
       ROW_NUMBER() OVER (PARTITION BY k_bigint ORDER BY id) AS rn
FROM streaming_row_number_keys
ORDER BY id

query
SELECT id, k_decimal,
       ROW_NUMBER() OVER (PARTITION BY k_decimal ORDER BY id) AS rn
FROM streaming_row_number_keys
ORDER BY id

query
SELECT id, k_date,
       ROW_NUMBER() OVER (PARTITION BY k_date ORDER BY id) AS rn
FROM streaming_row_number_keys
ORDER BY id

query
SELECT id, k_timestamp,
       ROW_NUMBER() OVER (PARTITION BY k_timestamp ORDER BY id) AS rn
FROM streaming_row_number_keys
ORDER BY id

query
SELECT id, k_string,
       ROW_NUMBER() OVER (PARTITION BY k_string ORDER BY id) AS rn
FROM streaming_row_number_keys
ORDER BY id

query
SELECT id, k_binary,
       ROW_NUMBER() OVER (PARTITION BY k_binary ORDER BY id) AS rn
FROM streaming_row_number_keys
ORDER BY id

-- The global counter crosses batches without a retained partition key.
query
SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn
FROM streaming_row_number_keys
ORDER BY id

-- Repeated results and carried columns survive batch-at-a-time execution.
query
SELECT id, k_string, payload,
       ROW_NUMBER() OVER (PARTITION BY k_string ORDER BY id) AS first_rank,
       ROW_NUMBER() OVER (PARTITION BY k_string ORDER BY id) AS second_rank
FROM streaming_row_number_keys
ORDER BY id

-- A mixed window retains the generic executor and its frame behaviour.
query
SELECT id, k_string,
       ROW_NUMBER() OVER (PARTITION BY k_string ORDER BY id) AS rn,
       COUNT(*) OVER (PARTITION BY k_string ORDER BY id
                      ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS n
FROM streaming_row_number_keys
ORDER BY id
