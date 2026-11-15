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

-- Spark 4.2 reverses the bytes of a binary argument and returns binary. Earlier versions cast it
-- to a string first, which reverse.sql covers. Comet has no native reverse for binary, so it runs
-- Spark's code through the codegen dispatcher, whether or not incompatible expressions are allowed.
-- MinSparkVersion: 4.2
-- ConfigMatrix: spark.comet.expression.Reverse.allowIncompatible=false,true

statement
CREATE TABLE test_reverse_binary(b binary) USING parquet

-- X'636166C3A9' is 'café' in UTF-8: reversed by byte, not by character, it is not valid UTF-8.
statement
INSERT INTO test_reverse_binary VALUES (X'CAFE'), (X''), (NULL), (X'01'), (X'636166C3A9')

query expect_dispatch(reverse)
SELECT reverse(b) FROM test_reverse_binary
