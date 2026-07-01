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

-- Routes overlay through the codegen dispatcher so behavior matches Spark exactly.

statement
CREATE TABLE test_overlay(s string) USING parquet

statement
INSERT INTO test_overlay VALUES ('Spark SQL'), ('abcdef'), (NULL)

query
SELECT s, overlay(s PLACING '_' FROM 2) FROM test_overlay

-- literal arguments with length
query
SELECT overlay('Spark SQL' PLACING 'CORE' FROM 7), overlay('Spark SQL' PLACING 'ANSI ' FROM 7 FOR 0)
