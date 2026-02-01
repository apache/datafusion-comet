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

-- ConfigMatrix: spark.sql.optimizer.inSetConversionThreshold=100,0
-- ConfigMatrix: parquet.enable.dictionary=false,true

-- test in(set)/not in(set)
statement
CREATE TABLE names(id int, name varchar(20)) USING parquet

statement
INSERT INTO names VALUES(1, 'James'), (1, 'Jones'), (2, 'Smith'), (3, 'Smith'), (NULL, 'Jones'), (4, NULL)

query
SELECT * FROM names WHERE id in (1, 2, 4, NULL)

query
SELECT * FROM names WHERE name in ('Smith', 'Brown', NULL)

query
SELECT * FROM names WHERE id not in (1)

query spark_answer_only
SELECT * FROM names WHERE name not in ('Smith', 'Brown', NULL)
