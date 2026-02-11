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
CREATE TABLE test_trunc_ts(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_ts VALUES (timestamp('2024-06-15 10:30:45')), (timestamp('2024-01-01 00:00:00')), (NULL)

query
SELECT date_trunc('year', ts) FROM test_trunc_ts

query
SELECT date_trunc('month', ts) FROM test_trunc_ts

query
SELECT date_trunc('day', ts) FROM test_trunc_ts

query
SELECT date_trunc('hour', ts) FROM test_trunc_ts

-- literal arguments
query ignore(https://github.com/apache/datafusion-comet/issues/3342)
SELECT date_trunc('year', timestamp('2024-06-15 10:30:45')), date_trunc('month', timestamp('2024-06-15 10:30:45')), date_trunc('day', timestamp('2024-06-15 10:30:45'))
