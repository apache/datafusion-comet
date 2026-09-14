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

-- Tests that str_to_map routes through the JVM codegen dispatcher when
-- spark.sql.legacy.truncateForEmptyRegexSplit is enabled. In legacy mode Spark truncates
-- trailing empty entries from the split result, which Comet's native str_to_map does not
-- honour. `CometStrToMap` marks the expression Incompatible when the flag is on and mixes in
-- [[CodegenDispatchFallback]], so the projection stays native (Spark's own `doGenCode` runs
-- inside the Comet kernel) while producing Spark-exact results.
-- See https://github.com/apache/datafusion-comet/issues/4477

-- Config: spark.sql.legacy.truncateForEmptyRegexSplit=true

-- trailing pair delimiter: legacy mode truncates the trailing empty entry; Comet delegates to
-- the codegen dispatcher.
query
SELECT str_to_map('a:1,b:2,', ',', ':')

-- column input is also handled via the codegen dispatcher
statement
CREATE TABLE test_str_to_map_legacy(s STRING, pair_delim STRING, key_value_delim STRING) USING parquet

statement
INSERT INTO test_str_to_map_legacy VALUES
  ('a:1,b:2,', ',', ':'),
  ('x:1;y:2;', ';', ':'),
  (NULL, ',', ':')

query
SELECT str_to_map(s, pair_delim, key_value_delim) FROM test_str_to_map_legacy
