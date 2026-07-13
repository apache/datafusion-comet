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

statement
CREATE TABLE test_bit_length(s string) USING parquet

statement
INSERT INTO test_bit_length VALUES (''), ('a'), ('hello'), (NULL), ('café')

query
SELECT bit_length(s) FROM test_bit_length

-- literal arguments
query
SELECT bit_length('hello'), bit_length(''), bit_length(NULL)

-- BinaryType input falls back to Spark; the native DataFusion impl rejects Binary at runtime,
-- so the serde gates Binary as Unsupported (matching the existing CometLength shape).
statement
CREATE TABLE test_bit_length_binary(b binary) USING parquet

statement
INSERT INTO test_bit_length_binary VALUES (X'48656c6c6f'), (X''), (NULL), (X'FF')

query expect_fallback(bit_length on BinaryType is not supported)
SELECT bit_length(b) FROM test_bit_length_binary

query expect_fallback(bit_length on BinaryType is not supported)
SELECT bit_length(X'48656c6c6f'), bit_length(CAST(NULL AS BINARY))
