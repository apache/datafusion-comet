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

-- MinSparkVersion: 4.2
-- Config: spark.sql.ansi.enabled=true
-- ConstantFolding: enabled

statement
CREATE TABLE shared_division(x BIGINT, p BOOLEAN, q BOOLEAN) USING parquet

statement
INSERT INTO shared_division SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1L, true, true), (0L, NULL, false) AS t(x, p, q)

-- Spark 4.2 computes the shared division before testing p and q in the first conjunct.
query expect_error_class(DIVIDE_BY_ZERO)
SELECT x FROM shared_division WHERE (p OR (q AND 10 DIV x > 0)) AND 10 DIV x < 100
