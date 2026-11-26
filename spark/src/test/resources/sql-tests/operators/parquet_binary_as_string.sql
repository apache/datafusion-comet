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

-- Config: spark.sql.parquet.binaryAsString=true
-- ConfigMatrix: spark.comet.parquet.rowFilterPushdown.enabled=false,true

statement
CREATE TABLE parquet_binary_as_string(value STRING) USING parquet

statement
INSERT INTO parquet_binary_as_string VALUES ('one'), ('two'), (NULL)

query
SELECT * FROM parquet_binary_as_string WHERE value = 'one'

query
SELECT * FROM parquet_binary_as_string WHERE value < 'two'

query
SELECT * FROM parquet_binary_as_string WHERE value LIKE 't%'

query
SELECT * FROM parquet_binary_as_string WHERE value NOT IN ('one')

query
SELECT * FROM parquet_binary_as_string WHERE value IS NULL
