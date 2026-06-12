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

-- When spark.sql.legacy.castComplexTypesToString.enabled is true Spark uses the legacy
-- formatter (`[...]` with NULL elements omitted). Comet only implements the default
-- (`{...}` with NULL elements rendered as "null"), so the cast must fall back to Spark.
-- The flag is internal in Spark 4.0 and defaults to false.

-- Config: spark.sql.legacy.castComplexTypesToString.enabled=true

query expect_fallback(spark.sql.legacy.castComplexTypesToString.enabled=true is not supported)
SELECT CAST(struct(1, 2, null) AS STRING)
