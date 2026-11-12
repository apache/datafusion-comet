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

-- Config: spark.sql.ansi.enabled=true

-- Column inputs ensure these errors occur during execution inside Comet.
statement
CREATE TABLE error_class_input(n int, d int, s string, arr array<int>, idx int) USING parquet

statement
INSERT INTO error_class_input SELECT 1, 0, 'invalid', array(1, 2), 3

query expect_error_class(DIVIDE_BY_ZERO)
SELECT n / d FROM error_class_input

query expect_error_class(CAST_INVALID_INPUT)
SELECT CAST(s AS INT) FROM error_class_input

query expect_error_class(INVALID_ARRAY_INDEX)
SELECT arr[idx] FROM error_class_input
