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

-- Spark 3.4.3 emits invalid Java for bit_count(boolean). Use interpreted Spark
-- reference execution; plain queries still assert Comet operator replacement.
-- Config: spark.sql.codegen.wholeStage=false
-- Config: spark.sql.codegen.factoryMode=NO_CODEGEN

statement
CREATE TABLE test_bit_count_types(l bigint, i int, s smallint, b tinyint, flag boolean) USING parquet

statement
INSERT INTO test_bit_count_types
SELECT /*+ COALESCE(1) */ l, i, s, b, flag
FROM VALUES
  (1111, 2222, 17, 7, true),
  (9223372036854775807, 2147483647, 32767, 127, true),
  (-9223372036854775808, -2147483648, -32768, -128, false),
  (-1, -1, -1, -1, false),
  (0, 0, 0, 0, false),
  (1, 1, 1, 1, true),
  (4294967293, 0, 0, 0, true),
  (4294967294, 0, 0, 0, false),
  (4294967295, 0, 0, 0, true),
  (4294967296, 0, 0, 0, false),
  (NULL, NULL, NULL, NULL, NULL)
AS input(l, i, s, b, flag)

query
SELECT bit_count(l), bit_count(i), bit_count(s), bit_count(b), bit_count(flag)
FROM test_bit_count_types

query
SELECT bit_count(true), bit_count(false) FROM test_bit_count_types
