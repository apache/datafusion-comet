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

-- Routes format_string through the codegen dispatcher so behavior matches Spark exactly.

statement
CREATE TABLE test_format_string(i int, s string) USING parquet

statement
INSERT INTO test_format_string VALUES (1, 'a'), (42, 'hello'), (NULL, 'x')

query
SELECT format_string('%d-%s', i, s) FROM test_format_string

-- literal arguments
query
SELECT format_string('%d-%s', 1, 'x'), format_string('%05.2f', double(3.14159)), format_string('%s', NULL)
