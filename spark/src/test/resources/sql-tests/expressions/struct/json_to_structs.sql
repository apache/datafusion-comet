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
CREATE TABLE test_from_json(j string) USING parquet

statement
INSERT INTO test_from_json VALUES ('{"a": 1, "b": "hello"}'), ('{}'), (NULL), ('{"a": null}'), ('invalid json')

query spark_answer_only
SELECT from_json(j, 'a INT, b STRING') FROM test_from_json

-- literal arguments
query spark_answer_only
SELECT from_json('{"a": 1, "b": "hello"}', 'a INT, b STRING')
