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

statement
CREATE TABLE test_hex(i int, l long, s string) USING parquet

statement
INSERT INTO test_hex VALUES (0, 0, ''), (255, 255, 'Spark'), (-1, -1, NULL), (NULL, NULL, 'ABC'), (2147483647, 9223372036854775807, 'a')

query
SELECT hex(i), hex(l), hex(s) FROM test_hex

-- literal arguments
query
SELECT hex(255), hex('Spark'), hex(NULL)
