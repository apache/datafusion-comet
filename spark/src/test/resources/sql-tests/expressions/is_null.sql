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
CREATE TABLE test_is_null(i int, s string, d double) USING parquet

statement
INSERT INTO test_is_null VALUES (1, 'a', 1.0), (NULL, NULL, NULL), (0, '', 0.0), (NULL, 'b', cast('NaN' as double))

query
SELECT i IS NULL, s IS NULL, d IS NULL FROM test_is_null

query
SELECT isnull(i), isnull(s), isnull(d) FROM test_is_null
