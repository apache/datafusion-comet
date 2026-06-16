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

-- try_to_binary is RuntimeReplaceable (TryEval(ToBinary(...))). Verify Comet runs it natively and
-- matches Spark, including the invalid-hex case which returns NULL.

-- MinSparkVersion: 3.5

statement
CREATE TABLE test_try_to_binary(s string) USING parquet

statement
INSERT INTO test_try_to_binary VALUES ('616263'), ('48656c6c6f'), ('zz'), (''), (NULL)

query
SELECT s, try_to_binary(s, 'hex') FROM test_try_to_binary

query
SELECT try_to_binary('616263', 'hex'), try_to_binary('not-hex', 'hex')
