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

-- Config: spark.sql.ansi.enabled=true

-- conv executes Spark's generated code inside Comet's codegen dispatcher.
statement
CREATE TABLE test_conv_ansi(id int, s string) USING parquet

statement
INSERT INTO test_conv_ansi VALUES
 (1, 'FF'), (2, 'FFFFFFFFFFFFFFFF'), (3, NULL), (4, 'FFFFFFFFFFFFFFFFF')

-- Valid input, the unsigned 64-bit boundary and NULL must execute inside Comet.
query
SELECT id, conv(s, 16, 10), conv(s, 16, -10) FROM test_conv_ansi WHERE id < 4

-- One more hex digit exceeds the unsigned 64-bit range.
query expect_error(ARITHMETIC_OVERFLOW)
SELECT conv(s, 16, 10) FROM test_conv_ansi WHERE id = 4

query expect_error(ARITHMETIC_OVERFLOW)
SELECT conv('FFFFFFFFFFFFFFFFF', 16, 10)
