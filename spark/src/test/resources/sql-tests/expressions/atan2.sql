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
CREATE TABLE test_atan2(y double, x double) USING parquet

statement
INSERT INTO test_atan2 VALUES (0.0, 1.0), (1.0, 0.0), (1.0, 1.0), (-1.0, -1.0), (0.0, 0.0), (NULL, 1.0), (1.0, NULL), (cast('NaN' as double), 1.0), (cast('Infinity' as double), 1.0)

query tolerance=1e-6
SELECT atan2(y, x) FROM test_atan2
