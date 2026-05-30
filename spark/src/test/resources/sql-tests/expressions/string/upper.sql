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

-- Routes Upper through the codegen dispatcher so behavior matches Spark exactly,
-- including locale-specific case mappings that the Rust scalar function does not implement.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_upper(s string) USING parquet

statement
INSERT INTO test_upper VALUES ('hello'), ('HELLO'), ('Hello World'), (''), (NULL), ('123abc')

query
SELECT upper(s) FROM test_upper

-- literal arguments
query
SELECT upper('hello'), upper(''), upper(NULL)

-- locale-sensitive characters: German sharp s and Turkish dotted/dotless I
statement
CREATE TABLE test_upper_unicode(s string) USING parquet

statement
INSERT INTO test_upper_unicode VALUES ('straße'), ('istanbul'), ('İstanbul'), ('ﬁnish')

query
SELECT upper(s) FROM test_upper_unicode
