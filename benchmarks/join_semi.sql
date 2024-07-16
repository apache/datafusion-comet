-- SQLBench-DS query 94 derived from TPC-DS query 94 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 1.
select
    count(distinct ws_order_number) as `order count`
from
    web_sales ws1
where exists (select *
              from web_sales ws2
              where ws1.ws_order_number = ws2.ws_order_number
                and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
order by count(distinct ws_order_number)
    LIMIT 100;

