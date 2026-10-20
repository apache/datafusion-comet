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
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Non-default string collations require Spark's comparator, including when the strings nest in
-- structs. With JVM codegen dispatch disabled these calls fall back to Spark.
-- MinSparkVersion: 4.0
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE array_sort_collation (a STRING, b STRING) USING parquet

statement
INSERT INTO array_sort_collation VALUES ('a', 'B'), ('B', 'a')

query expect_fallback(expression has no native path)
SELECT array_sort(array(CAST(a AS STRING COLLATE UTF8_LCASE),
                        CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM array_sort_collation

query expect_fallback(expression has no native path)
SELECT array_sort(array(named_struct('s', CAST(a AS STRING COLLATE UTF8_LCASE)),
                        named_struct('s', CAST(b AS STRING COLLATE UTF8_LCASE))))
FROM array_sort_collation
