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

-- Test RLIKE with regexp allowIncompatible enabled (happy path)
-- Config: spark.comet.expression.regexp.allowIncompatible=true
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_rlike_enabled(s string) USING parquet

statement
INSERT INTO test_rlike_enabled VALUES ('hello'), ('12345'), (''), (NULL), ('Hello World'), ('abc123')

query
SELECT s RLIKE '^[0-9]+$' FROM test_rlike_enabled

query
SELECT s RLIKE '^[a-z]+$' FROM test_rlike_enabled

query
SELECT s RLIKE '' FROM test_rlike_enabled

-- literal arguments
query ignore(https://github.com/apache/datafusion-comet/issues/3343)
SELECT 'hello' RLIKE '^[a-z]+$', '12345' RLIKE '^[a-z]+$', '' RLIKE '', NULL RLIKE 'a'
