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

statement
CREATE TABLE test_cast_array_to_string(
  id int,
  floats array<float>,
  doubles array<double>,
  binaries array<binary>
) USING parquet

statement
INSERT INTO test_cast_array_to_string VALUES
  (
    1,
    array(cast('3.4028235E38' as float), cast('-3.4028235E38' as float), cast('1.4E-45' as float)),
    array(cast('1.7976931348623157E308' as double), cast('-1.7976931348623157E308' as double), cast('4.9E-324' as double)),
    array(X'616263', X'', X'0001027F')
  ),
  (
    2,
    array(cast('NaN' as float), cast('Infinity' as float), cast('-Infinity' as float)),
    array(cast('NaN' as double), cast('Infinity' as double), cast('-Infinity' as double)),
    array(cast(null as binary), X'FFFE', X'0A0D')
  ),
  (
    3,
    array(cast(null as float), cast(-0.0 as float), cast(0.0 as float)),
    array(cast(null as double), cast(-0.0 as double), cast(0.0 as double)),
    null
  ),
  (
    4,
    cast(array() as array<float>),
    cast(array() as array<double>),
    cast(array() as array<binary>)
  )

query
SELECT cast(floats as string), cast(doubles as string), cast(binaries as string), id
FROM test_cast_array_to_string
ORDER BY id

query
SELECT
  cast(array(cast('1.4E-45' as float), cast('NaN' as float), cast(null as float)) as string),
  cast(array(cast('4.9E-324' as double), cast('-Infinity' as double), cast(null as double)) as string),
  cast(array(X'616263', X'', cast(null as binary)) as string)
