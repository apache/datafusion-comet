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

-- A collated string field inside a struct element compares by its collation in Spark. Nested
-- elements stay on the codegen dispatcher, which keeps that. Collation syntax requires Spark 4.0+.
-- MinSparkVersion: 4.0

statement
CREATE TABLE ac_collation(d DOUBLE, s STRING) USING parquet

statement
INSERT INTO ac_collation VALUES (0.0D, 'a'), (1.0D, 'b'), (NULL, NULL)

query expect_dispatch(array_contains)
SELECT array_contains(
  array(named_struct('d', d, 's', CAST(s AS STRING COLLATE UTF8_LCASE))),
  named_struct('d', d, 's', CAST(upper(s) AS STRING COLLATE UTF8_LCASE)))
FROM ac_collation
