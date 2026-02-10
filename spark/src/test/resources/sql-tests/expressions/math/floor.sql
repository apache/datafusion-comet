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
CREATE TABLE test_floor(f float, d double) USING parquet

statement
INSERT INTO test_floor VALUES (1.9, 1.9), (-1.1, -1.1), (0.0, 0.0), (1.0, 1.0), (NULL, NULL), (cast('NaN' as float), cast('NaN' as double)), (cast('Infinity' as float), cast('Infinity' as double))

query
SELECT floor(f), floor(d) FROM test_floor

-- literal arguments
query
SELECT floor(1.9), floor(-1.1), floor(0.0), floor(NULL)
