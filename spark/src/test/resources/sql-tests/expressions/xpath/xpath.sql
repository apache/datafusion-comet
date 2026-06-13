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

-- The xpath family has no native (rust) implementation; these extend Spark's CodegenFallback and
-- stay native via the codegen dispatcher.

statement
CREATE TABLE test_xpath(s STRING) USING parquet

statement
INSERT INTO test_xpath VALUES
  ('<a><b>1</b><b>2</b></a>'), ('<a><b>9</b></a>'), ('<a/>'), (NULL)

query
SELECT xpath_boolean(s, 'a/b') FROM test_xpath

query
SELECT xpath_short(s, 'sum(a/b)'), xpath_int(s, 'sum(a/b)'), xpath_long(s, 'sum(a/b)') FROM test_xpath

query
SELECT xpath_float(s, 'sum(a/b)'), xpath_double(s, 'sum(a/b)') FROM test_xpath

query
SELECT xpath_string(s, 'a/b[1]') FROM test_xpath

query
SELECT xpath(s, 'a/b/text()') FROM test_xpath
