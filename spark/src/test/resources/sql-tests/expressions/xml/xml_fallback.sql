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

-- With the codegen dispatcher disabled, from_xml has no native path and falls back to Spark.
-- XML SQL functions were added in Spark 4.0.

-- MinSparkVersion: 4.0

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE test_xml_fallback(s STRING) USING parquet

statement
INSERT INTO test_xml_fallback VALUES
  ('<a><b>1</b><b>2</b></a>'), ('<a><b>9</b></a>'), ('<a/>'), (NULL)

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT from_xml(s, 'b ARRAY<INT>') FROM test_xml_fallback
