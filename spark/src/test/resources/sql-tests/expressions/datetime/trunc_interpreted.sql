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

-- Spark's `eval` for `trunc` and `date_trunc` reads a non-literal format first and returns NULL
-- for an invalid one without evaluating the date or timestamp, while its generated code, like the
-- native kernel, evaluates both. An imperative aggregate (`collect_list`) evaluates its input
-- through `eval`, so there the invalid value on the row with an invalid format must not raise.
-- The expression runs through the codegen dispatcher in that context, which evaluates it the
-- same way.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.expression.TruncDate.allowIncompatible=true
-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true

statement
CREATE TABLE test_trunc_interpreted(s string, fmt string) USING parquet

-- One partition, so the rows share one batch.
statement
INSERT INTO test_trunc_interpreted SELECT * FROM VALUES ('not-a-date', 'bogus'), ('2024-05-17', 'year') AS t(s, fmt) DISTRIBUTE BY 1

query expect_dispatch(trunc)
SELECT collect_list(trunc(CAST(s AS date), fmt)) FROM test_trunc_interpreted

query expect_dispatch(date_trunc)
SELECT collect_list(date_trunc(fmt, CAST(s AS timestamp))) FROM test_trunc_interpreted
