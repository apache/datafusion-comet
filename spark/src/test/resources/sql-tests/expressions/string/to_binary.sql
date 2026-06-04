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

-- to_binary with 'hex' format lowers to Unhex, which Comet accelerates.
-- to_binary with 'utf-8' and 'base64' formats fall back to Spark.

statement
CREATE TABLE test_to_binary(s string) USING parquet

statement
INSERT INTO test_to_binary VALUES ('537061726B'), ('41'), ('0A1B'), (''), (NULL)

-- hex format: accelerated via Unhex
query
SELECT to_binary(s, 'hex') FROM test_to_binary

-- literal hex arguments
query
SELECT to_binary('41', 'hex'), to_binary('0A1B', 'hex'), to_binary('', 'hex'), to_binary(NULL, 'hex')

-- utf-8 format falls back to Spark
query spark_answer_only
SELECT to_binary(s, 'utf-8') FROM test_to_binary

-- base64 format falls back to Spark
query spark_answer_only
SELECT to_binary(s, 'base64') FROM test_to_binary
