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

-- ExcludeRules: org.apache.spark.sql.catalyst.optimizer.NullPropagation

statement
CREATE TABLE abs_null_literal(v INT) USING parquet

statement
INSERT INTO abs_null_literal VALUES (1), (-2), (NULL)

-- Excluding ConstantFolding alone is insufficient: NullPropagation would replace
-- abs(NULL) with a NULL literal. Pin abs itself so that such a rewrite fails this test.
query expect_native(abs)
SELECT abs(CAST(NULL AS INT)) FROM abs_null_literal
