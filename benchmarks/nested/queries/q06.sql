-- CometBench-H nested query 6 derived from TPC-H query 6 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- using default substitutions

select
	sum(lineitem_data.l_extendedprice * lineitem_data.l_discount) as revenue
from
	(select l_orderkey, l_partkey, l_suppkey, l_shipdate, explode(lineitem_data) as lineitem_data from lineitem) as lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1' year
	and lineitem_data.l_discount between .06 - 0.01 and .06 + 0.01
	and lineitem_data.l_quantity < 24
