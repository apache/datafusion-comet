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

-- Routes months_between through the codegen dispatcher.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_months_between(t1 timestamp, t2 timestamp) USING parquet

statement
INSERT INTO test_months_between VALUES
  (timestamp('2024-06-15 10:30:00'), timestamp('2024-01-15 10:30:00')),
  (timestamp('2024-01-31 00:00:00'), timestamp('2024-02-29 00:00:00')),
  (timestamp('1970-01-01 00:00:00'), timestamp('1970-01-01 00:00:00')),
  (NULL, timestamp('2024-01-01 00:00:00')),
  (timestamp('2024-01-01 00:00:00'), NULL)

query
SELECT months_between(t1, t2) FROM test_months_between

query
SELECT months_between(t1, t2, false) FROM test_months_between

-- literal arguments
query
SELECT months_between(timestamp('1997-02-28 10:30:00'), timestamp('1996-10-30 00:00:00'))
