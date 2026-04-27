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
CREATE TABLE test_ceil(f float, d double, dec DECIMAL(5, 2)) USING parquet

statement
INSERT INTO test_ceil VALUES (1.1, 1.1, 1.10), (-1.1, -1.1, -1.10), (0.0, 0.0, 0.00), (1.0, 1.0, 1.00), (NULL, NULL, NULL), (cast('NaN' as float), cast('NaN' as double), NULL), (cast('Infinity' as float), cast('Infinity' as double), NULL)

query
SELECT ceil(f), ceil(d), ceil(dec) FROM test_ceil

-- literal arguments
-- IgnoreFromSparkVersion: 4.1 https://github.com/apache/datafusion-comet/issues/4098
query
SELECT ceil(1.1), ceil(-1.1), ceil(0.0), ceil(NULL), ceil(cast(1.10 as DECIMAL(5, 2))), ceil(cast(-1.10 as DECIMAL(5, 2)))
