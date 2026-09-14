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

-- Companion to trunc_date.sql exercising the native (Rust) date_trunc implementation with a
-- non-literal format. A non-literal format is Incompatible by default because the native impl
-- throws on an invalid format string instead of returning NULL like Spark, so it is routed
-- through the codegen dispatcher. With allowIncompatible enabled the native UDF runs instead. As
-- long as every format value is valid, the native results match Spark.

-- Config: spark.comet.expression.TruncDate.allowIncompatible=true

statement
CREATE TABLE test_trunc_date_fmt(d date, fmt string) USING parquet

statement
INSERT INTO test_trunc_date_fmt VALUES (date('2024-06-15'), 'year'), (date('2024-06-15'), 'month'), (date('2024-06-15'), 'quarter'), (date('2024-06-15'), 'week'), (NULL, 'year')

query
SELECT trunc(d, fmt) FROM test_trunc_date_fmt
