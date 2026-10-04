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

-- The interpreted-evaluation check for `trunc` (see trunc_interpreted.sql) runs ahead of the
-- collation opt-in, so a collated non-literal format cannot route the expression to the native
-- kernel through `allowIncompatible` where Spark evaluates it through `eval`.

-- MinSparkVersion: 4.0
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.expression.TruncDate.allowIncompatible=true

statement
CREATE TABLE test_trunc_interpreted_collation(s string, fmt string) USING parquet

statement
INSERT INTO test_trunc_interpreted_collation SELECT * FROM VALUES ('not-a-date', 'bogus'), ('2024-05-17', 'year') AS t(s, fmt) DISTRIBUTE BY 1

query expect_dispatch(trunc)
SELECT collect_list(trunc(CAST(s AS date), CAST(fmt AS STRING COLLATE UTF8_LCASE))) FROM test_trunc_interpreted_collation
