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

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_unix_date(d date) USING parquet

statement
INSERT INTO test_unix_date VALUES (date('1970-01-01')), (date('2024-01-15')), (date('1969-12-31')), (NULL)

query spark_answer_only
SELECT unix_date(d) FROM test_unix_date

-- literal arguments
query spark_answer_only
SELECT unix_date(date('1970-01-01')), unix_date(date('2024-01-15')), unix_date(date('1969-12-31')), unix_date(NULL)
