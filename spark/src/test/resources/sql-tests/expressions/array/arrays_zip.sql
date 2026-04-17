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

-- ConfigMatrix: parquet.enable.dictionary=false,true

-- Basic usage with arrays of same length
query
SELECT arrays_zip(array(1, 2, 3), array(2, 3, 4));

-- Arrays with different lengths
query
SELECT arrays_zip(array(1, 2, 3), array('a', 'b'));

-- With null values
query
SELECT arrays_zip(array(1, null, 3), array('x', 'y', 'z'));

-- basic: two integer arrays of equal length
query
select arrays_zip(array(1, 2, 3), array(10, 20, 30));

-- basic: two arrays with different element types (int + string)
query
select arrays_zip(array(1, 2, 3), array('a', 'b', 'c'));

-- three arrays of equal length
query
SELECT arrays_zip(array(1, 2), array(2, 3), array(3, 4));

-- three arrays of equal length
query
select arrays_zip(array(1, 2, 3), array(10, 20, 30), array(100, 200, 300));

-- four arrays of equal length
query
select arrays_zip(array(1), array(2), array(3), array(4));

-- mixed element types: float + boolean
query
select arrays_zip(array(1.5, 2.5), array(true, false));

-- different length arrays: shorter array padded with NULLs
query
select arrays_zip(array(1, 2), array(3, 4, 5));

-- different length arrays: first longer
query
select arrays_zip(array(1, 2, 3), array(10));

-- different length: one single element, other three elements
query
select arrays_zip(array(1), array('a', 'b', 'c'));

-- empty arrays
query
select arrays_zip(array(), array());

-- one empty, one non-empty
query
select arrays_zip(array(), array(1, 2, 3));

-- NULL elements inside arrays
query
select arrays_zip(array(1, null, 3), array('a', 'b', 'c'));

-- all NULL elements
query
select arrays_zip(array(cast(NULL AS int), NULL, NULL), array(cast(NULL AS string), NULL, NULL));

-- both args are NULL (entire list null)
query
select arrays_zip(cast(NULL AS array<int>), cast(NULL AS array<int>));

-- single element arrays
query
select arrays_zip(array(42), array('hello'));

-- single argument
query
SELECT arrays_zip(null)

query
select arrays_zip(cast(NULL AS array<int>));

query
select arrays_zip(array());

query
select arrays_zip(array(1, 2, 3));

-- one arg is NULL list, other is real array
query
select arrays_zip(cast(NULL AS array<int>), array(1, 2, 3));

-- real array + NULL list
query
select arrays_zip(array(1, 2), cast(NULL AS array<int>));

-- w/ names
statement
CREATE TABLE test_arrays_zip(a array<int>, b array<int>) USING parquet

statement
-- column-level test with multiple rows
INSERT INTO test_arrays_zip VALUES (array(1, 2), array(10, 20)), (array(3, 4, 5), array(30)), (array(6), array(60, 70))

-- column-level test with NULL rows
INSERT INTO test_arrays_zip VALUES (array(1, 2), array(10, 20)), (cast(NULL AS array<int>), array(30, 40)), (array(5, 6), cast(NULL AS array<int>))

INSERT INTO test_arrays_zip VALUES (array(1), array(10, 20)), (array(2, 3), array(30))

query
select arrays_zip(a, b) FROM test_arrays_zip

-- single argument
query
select arrays_zip(a) FROM test_arrays_zip

query
select arrays_zip(b) FROM test_arrays_zip

-- real array + NULL list
query
SELECT arrays_zip(a, b) FROM (SELECT array(1, 2, 3) as a, null as b)

query
SELECT arrays_zip(b, a) FROM (SELECT array(1, 2, 3) as a, null as b)

query
SELECT arrays_zip(a) FROM (SELECT array(1, 2, 3) as a, null as b)

query
SELECT arrays_zip(b) FROM (SELECT array(1, 2, 3) as a, null as b)
