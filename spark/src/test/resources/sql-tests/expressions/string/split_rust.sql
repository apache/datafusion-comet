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

-- Test split with Rust regexp engine (patterns expected to fallback)
-- Config: spark.comet.exec.regexp.engine=rust

statement
CREATE TABLE test_split_rust(s string) USING parquet

statement
INSERT INTO test_split_rust VALUES ('one,two,three'), ('hello'), (''), (NULL), ('a::b::c')

query expect_fallback(is not fully compatible with Spark)
SELECT split(s, ',', -1) FROM test_split_rust

query expect_fallback(is not fully compatible with Spark)
SELECT split(s, '::', -1) FROM test_split_rust
