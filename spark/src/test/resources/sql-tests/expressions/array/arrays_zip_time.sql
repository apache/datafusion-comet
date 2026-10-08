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

-- TIME elements have no native arrays_zip kernel and run through the JVM codegen dispatcher.
query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(TIME '23:59:59.999999', TIME '2:0:3'))

statement
CREATE TABLE test_arrays_zip_time(hours int, minutes int, secs decimal(16, 6)) USING parquet

statement
INSERT INTO test_arrays_zip_time VALUES (0, 0, 0.000000), (23, 59, 59.999999), (12, NULL, 30.000000)

query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(make_time(hours, minutes, secs), NULL), array(hours)) FROM test_arrays_zip_time
