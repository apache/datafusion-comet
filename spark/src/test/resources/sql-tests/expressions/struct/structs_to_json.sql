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

-- Config: spark.comet.exec.json.engine=rust
-- ConfigMatrix: spark.sql.jsonGenerator.ignoreNullFields=false,true

statement
CREATE TABLE test_to_json(a int, b string, f float, d double) USING parquet

statement
INSERT INTO test_to_json VALUES (1, 'hello', cast('NaN' as float), cast('Infinity' as double)), (NULL, NULL, cast('NaN' as float), cast('Infinity' as double)), (NULL, NULL, NULL, NULL), (0, '', 0.0, 0.0), (2, 'hello', cast('NaN' as float), cast('-Infinity' as double))

query
SELECT to_json(named_struct('a', a, 'b', b, 'f', f, 'd', d)) FROM test_to_json

-- literal arguments
query
SELECT to_json(named_struct('a', 1, 'b', 'hello'))

-- to_json with options and with array fields are not supported by the native (rust) path, so they
-- route to the codegen (java) engine, which runs Spark's own implementation inside Comet.
query
SELECT to_json(named_struct('a', a, 'b', b), map('timestampFormat', 'dd/MM/yyyy')) FROM test_to_json

query
SELECT to_json(named_struct('a', a, 'b', array(b))) FROM test_to_json
