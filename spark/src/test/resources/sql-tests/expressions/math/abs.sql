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
CREATE TABLE test_abs(i int, l long, f float, d double) USING parquet

statement
INSERT INTO test_abs VALUES (1, 1, 1.5, 1.5), (-1, -1, -1.5, -1.5), (0, 0, 0.0, 0.0), (NULL, NULL, NULL, NULL), (2147483647, 9223372036854775807, cast('Infinity' as float), cast('NaN' as double))

query
SELECT abs(i), abs(l), abs(f), abs(d) FROM test_abs

-- literal arguments
query
SELECT abs(-5), abs(-1.5), abs(0), abs(NULL)
