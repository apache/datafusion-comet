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

-- ConfigMatrix: spark.sql.function.concatBinaryAsString=false,true

statement
CREATE TABLE test_concat_binary_as_string(id int, c1 binary, c2 binary, c3 binary) USING parquet

statement
INSERT INTO test_concat_binary_as_string VALUES
  (1, X'6162', X'6364', X'65'),
  (2, X'FF', X'FE41', X'80'),
  (3, X'', X'00', NULL),
  (4, NULL, X'01', X'02')

-- false keeps all-binary concat as BinaryType. true inserts BinaryType-to-StringType casts.
-- Both expression trees run through Spark's generated code inside the Comet pipeline so the
-- true path preserves malformed bytes instead of normalizing them to UTF-8 replacement bytes.
query
SELECT id, hex(concat(c1, c2))
FROM test_concat_binary_as_string
ORDER BY id

query
SELECT id, hex(concat(c1, c2, c3))
FROM test_concat_binary_as_string
ORDER BY id

query
SELECT
  hex(concat(X'FF', X'FE41')),
  concat(CAST(NULL AS BINARY), X'01') IS NULL
