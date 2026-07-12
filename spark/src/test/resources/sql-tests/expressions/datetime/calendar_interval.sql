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

-- Config: spark.comet.exec.localTableScan.enabled=true

query
SELECT * FROM VALUES
  (make_interval(1, 2, 3, 4, 5, 6, 7.008009)),
  (make_interval(30, 25, 0, -100, 40, 80, 299.889987299)),
  (make_interval(0, -1, 0, 1, 0, 0, -1)),
  (CAST(NULL AS INTERVAL))
AS test_calendar_interval(i)
