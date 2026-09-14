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

-- Routes InitCap through the codegen dispatcher so behavior matches Spark exactly,
-- including the hyphen-as-word-separator case where the Rust scalar function diverges
-- (see https://github.com/apache/datafusion-comet/issues/1052).
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_initcap(s string) USING parquet

statement
INSERT INTO test_initcap VALUES ('hello world'), ('HELLO WORLD'), (''), (NULL), ('hello-world'), ('123abc'), ('  spaces  ')

query
SELECT initcap(s) FROM test_initcap

-- literal arguments
query
SELECT initcap('hello world'), initcap(''), initcap(NULL)

-- hyphen and other word separators - the divergence the codegen dispatcher fixes
statement
CREATE TABLE test_initcap_separators(s string) USING parquet

statement
INSERT INTO test_initcap_separators VALUES ('robert rose-smith'), ('foo.bar'), ('a_b_c'), ("o'reilly")

query
SELECT initcap(s) FROM test_initcap_separators
