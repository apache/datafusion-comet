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

-- CSV structured-text functions (from_csv, schema_of_csv). These have no native (rust)
-- implementation; they extend Spark's CodegenFallback and stay native via the codegen
-- dispatcher.

statement
CREATE TABLE test_csv(s STRING) USING parquet

statement
INSERT INTO test_csv VALUES ('1,abc'), ('2,def'), (''), (NULL)

-- column argument
query
SELECT from_csv(s, 'a INT, b STRING') FROM test_csv

-- literal argument
query
SELECT schema_of_csv('1,abc')
