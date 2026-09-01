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

-- BinaryType has no native path, so it routes through the codegen dispatcher (Spark's own
-- `doGenCode`, i.e. `numBytes()`) instead of falling back to Spark.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_length(s string) USING parquet

statement
INSERT INTO test_length VALUES (''), ('a'), ('hello'), (NULL), ('café')

query
SELECT length(s), char_length(s) FROM test_length

-- literal arguments
query
SELECT length('hello'), length(''), length(NULL)

-- BinaryType input routes through the codegen dispatcher and stays inside Comet
statement
CREATE TABLE test_length_binary(b binary) USING parquet

statement
INSERT INTO test_length_binary VALUES (X'48656c6c6f'), (X''), (NULL), (X'FF')

query
SELECT length(b) FROM test_length_binary

query
SELECT length(X'48656c6c6f'), length(CAST(NULL AS BINARY))
