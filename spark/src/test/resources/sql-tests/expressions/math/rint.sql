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
CREATE TABLE test_rint(d double) USING parquet

statement
INSERT INTO test_rint VALUES (1.5), (2.5), (-1.5), (0.0), (3.7), (NULL), (cast('NaN' as double)), (cast('Infinity' as double))

-- column input
query
SELECT rint(d) FROM test_rint

-- literal arguments
query
SELECT rint(1.5), rint(2.5), rint(-1.5), rint(0.0), rint(3.7), rint(NULL)
