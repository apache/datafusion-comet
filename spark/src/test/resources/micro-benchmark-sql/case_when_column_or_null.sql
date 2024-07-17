-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

select
    sum(case when (ss_quantity=1) then ss_sales_price else null end) sun_sales,
    sum(case when (ss_quantity=2) then ss_sales_price else null end) mon_sales,
    sum(case when (ss_quantity=3) then ss_sales_price else null end) tue_sales,
    sum(case when (ss_quantity=4) then ss_sales_price else null end) wed_sales,
    sum(case when (ss_quantity=5) then ss_sales_price else null end) thu_sales,
    sum(case when (ss_quantity=6) then ss_sales_price else null end) fri_sales,
    sum(case when (ss_quantity=7) then ss_sales_price else null end) sat_sales
from store_sales;
