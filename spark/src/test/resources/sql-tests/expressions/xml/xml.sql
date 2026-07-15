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

-- XML SQL functions (from_xml, to_xml, schema_of_xml) were added in Spark 4.0. They have no
-- native (rust) implementation; they stay native via the codegen dispatcher.

-- MinSparkVersion: 4.0

statement
CREATE TABLE test_xml(s STRING) USING parquet

statement
INSERT INTO test_xml VALUES
  ('<a><b>1</b><b>2</b></a>'), ('<a><b>9</b></a>'), ('<a/>'), (NULL)

query
SELECT from_xml(s, 'b ARRAY<INT>') FROM test_xml

query
SELECT to_xml(named_struct('a', 1, 'b', s)) FROM test_xml

-- literal argument
query
SELECT schema_of_xml('<a><b>1</b></a>')
