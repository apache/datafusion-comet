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
CREATE TABLE test_log(d double) USING parquet

statement
INSERT INTO test_log VALUES (1.0), (2.718281828459045), (10.0), (0.5), (NULL), (cast('NaN' as double)), (cast('Infinity' as double))

query tolerance=1e-6
SELECT ln(d) FROM test_log

query tolerance=1e-6
SELECT log(10.0, d) FROM test_log

-- column + literal (log base from column)
query tolerance=1e-6
SELECT log(d, 10.0) FROM test_log

-- literal (1-arg form)
query tolerance=1e-6
SELECT ln(1.0), ln(2.718281828459045), ln(10.0), ln(NULL)

-- literal + literal (2-arg form)
query tolerance=1e-6
SELECT log(10.0, 100.0), log(2.0, 8.0), log(10.0, 1.0), log(NULL, 10.0)
