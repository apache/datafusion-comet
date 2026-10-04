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

-- MinSparkVersion: 4.0

-- Config: spark.comet.exec.range.enabled=true
-- Config: spark.comet.sparkToColumnar.enabled=true
-- Config: spark.comet.sparkToColumnar.supportedOperatorList=Range
-- Config: spark.sql.caseSensitive=false

-- Spark 4.x compares struct fields by position, irrespective of field names.
query
SELECT named_struct('x', CAST(id AS DOUBLE), 'y', CAST(NULL AS DOUBLE)) =
       named_struct('y', CAST(NULL AS DOUBLE), 'x', CAST(id AS DOUBLE))
FROM range(8)

-- Equal nullability must not bypass field-name alignment. Native Range makes every field
-- non-nullable; id=1 compares equal, while id=2 detects an accidental name-based reorder.
query expect_native(equalto)
SELECT named_struct('x', CAST(id AS DOUBLE), 'y', 1D) =
       named_struct('y', 1D, 'x', CAST(id AS DOUBLE)),
       array(named_struct('x', CAST(id AS DOUBLE), 'y', 1D)) =
       array(named_struct('y', 1D, 'x', CAST(id AS DOUBLE)))
FROM range(1, 3)
