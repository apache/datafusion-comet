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

-- MinSparkVersion: 4.1
-- Config: spark.sql.timeType.enabled=true

-- time - time rewrites to SubtractTimes -> StaticInvoke(DateTimeUtils.subtractTimes).
-- The shim routes it through the JVM codegen dispatcher; the result is
-- DayTimeIntervalType(HOUR, SECOND), which Comet's projection layer does not accept as an
-- output type yet, so these queries currently fall back to Spark. spark_answer_only pins
-- the semantics until DayTimeIntervalType is broadly supported.

statement
CREATE TABLE test_time_sub(h1 int, m1 int, s1 decimal(16,6), h2 int, m2 int, s2 decimal(16,6)) USING parquet

statement
INSERT INTO test_time_sub VALUES
  (1, 0, 0.000000, 2, 15, 30.000000),
  (10, 0, 0.000000, 9, 59, 59.500000),
  (0, 0, 0.000000, 23, 59, 59.999999),
  (NULL, 0, 0.000000, 0, 0, 0.000000)

query spark_answer_only
SELECT make_time(h2, m2, s2) - make_time(h1, m1, s1) FROM test_time_sub

query spark_answer_only
SELECT TIME '02:15:30' - TIME '01:00:00'

query spark_answer_only
SELECT TIME '09:59:59.5' - TIME '10:00:00'
