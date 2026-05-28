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

-- Routes add_months through the codegen dispatcher. Spark's own AddMonths.doGenCode
-- runs inside the Janino-compiled kernel.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_add_months(d date, n int) USING parquet

statement
INSERT INTO test_add_months VALUES
  (date('2024-01-15'), 1),
  (date('2024-01-31'), 1),
  (date('2024-12-15'), -13),
  (date('1970-01-01'), 0),
  (NULL, 1),
  (date('2024-06-15'), NULL)

query
SELECT add_months(d, n) FROM test_add_months

query
SELECT add_months(d, 12) FROM test_add_months

-- literal arguments
query
SELECT add_months(date('2024-02-29'), 12)
