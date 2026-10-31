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

-- cast(string as decimal) in ANSI mode. A zero with more integral digits than any decimal can
-- hold is rejected with NUMERIC_OUT_OF_SUPPORTED_RANGE, and Spark raises it as a
-- SparkArithmeticException from outOfDecimalTypeRangeError. Comet must throw the same class.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_cast_str_dec_ansi_range(s string) USING parquet

statement
INSERT INTO test_cast_str_dec_ansi_range VALUES ('0e38')

query expect_error_class(NUMERIC_OUT_OF_SUPPORTED_RANGE)
SELECT cast(s as decimal(10,2)) FROM test_cast_str_dec_ansi_range
