-- CometBench-H nested query 4 derived from TPC-H query 4 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- using default substitutions

select
	orders_data.o_orderpriority,
	count(*) as order_count
from
	(select o_orderkey, o_custkey, o_orderdate, explode(orders_data) as orders_data from orders) as orders
where
	o_orderdate >= date '1993-07-01'
	and o_orderdate < date '1993-07-01' + interval '3' month
	and exists (
		select
			*
		from
			(select l_orderkey, l_partkey, l_suppkey, l_shipdate, explode(lineitem_data) as lineitem_data from lineitem) as lineitem
		where
			l_orderkey = o_orderkey
			and lineitem_data.l_commitdate < lineitem_data.l_receiptdate
	)
group by
	orders_data.o_orderpriority
order by
	orders_data.o_orderpriority
