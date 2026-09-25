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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.sql.legacy.castComplexTypesToString.enabled=true
-- ConfigMatrix: spark.comet.expression.Cast.allowIncompatible=false,true

statement
CREATE TABLE routing_cast(i INT, b BOOLEAN, a ARRAY<INT>, s STRUCT<v: INT>, m MAP<STRING, INT>) USING parquet

statement
INSERT INTO routing_cast VALUES
  (1, true, array(1, null, 3), named_struct('v', 1), map('a', 1, 'b', null)),
  (0, false, array(), named_struct('v', null), map()),
  (NULL, NULL, NULL, NULL, NULL)

-- Compatible casts stay native regardless of dispatcher and opt-in settings.
query expect_native(cast)
SELECT CAST(i AS BIGINT) FROM routing_cast

-- These casts have no native path, even with allowIncompatible enabled.
query expect_fallback(cast: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT CAST(b AS DECIMAL(10, 2)) FROM routing_cast

-- Legacy complex-to-string formatting requires Spark's implementation.
query expect_fallback(cast: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT CAST(a AS STRING) FROM routing_cast

query expect_fallback(cast: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT CAST(s AS STRING) FROM routing_cast

query expect_fallback(cast: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT CAST(m AS STRING) FROM routing_cast
