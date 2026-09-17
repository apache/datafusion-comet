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

-- ConfigMatrix: spark.comet.exec.scalaUDF.codegen.enabled=false,true
-- Config: spark.comet.expression.ArrayJoin.allowIncompatible=false
-- Config: spark.comet.expression.RegExpReplace.allowIncompatible=false
-- Config: spark.sql.codegen.factoryMode=NO_CODEGEN
-- Config: spark.sql.codegen.wholeStage=false

-- ArrayJoin.eval evaluates the array before the replacement. Its generated code does the
-- reverse, and before Spark 4.2 it also has different null results for some input schemas.
-- Preserve interpreted execution, including when a parent could dispatch the whole subtree.
statement
CREATE TABLE test_aj_interpreted(arr array<string>, delim string, nr string, nested array<array<string>>) USING parquet

statement
INSERT INTO test_aj_interpreted VALUES
  (array('a', NULL, 'b'), ',', 'X', array(array('a', NULL, 'b'))),
  (array('a', NULL, 'b'), ',', NULL, array(array('a', NULL, 'b')))

query expect_fallback(NO_CODEGEN)
SELECT array_join(arr, delim, nr) FROM test_aj_interpreted WHERE arr IS NOT NULL AND delim IS NOT NULL

query expect_fallback(NO_CODEGEN)
SELECT length(array_join(arr, delim, nr)) FROM test_aj_interpreted WHERE arr IS NOT NULL AND delim IS NOT NULL

query expect_fallback(NO_CODEGEN)
SELECT regexp_replace(array_join(arr, delim, nr), delim, nr) FROM test_aj_interpreted WHERE arr IS NOT NULL AND delim IS NOT NULL

query expect_fallback(NO_CODEGEN)
SELECT array_join(element_at(nested, 1), ',', nr) FROM test_aj_interpreted

-- Keep unrelated expressions native in this mode. The array_join queries above intentionally
-- fall back, so their fallback assertions and the Scala routing test guard the error case.
query
SELECT element_at(nested, 1) FROM test_aj_interpreted

-- Even a NULL replacement must not hide the array argument's error in interpreted execution.
query expect_error(INVALID_INDEX_OF_ZERO)
SELECT array_join(element_at(nested, 0), ',', nr) FROM test_aj_interpreted WHERE nr IS NULL
