-- CometBench-H nested query 12 derived from TPC-H query 12 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- using default substitutions

select
	lineitem_data.l_shipmode,
	sum(case
		when orders_data.o_orderpriority = '1-URGENT'
			or orders_data.o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when orders_data.o_orderpriority <> '1-URGENT'
			and orders_data.o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	(select o_orderkey, o_custkey, o_orderdate, explode(orders_data) as orders_data from orders) as orders,
	(select l_orderkey, l_partkey, l_suppkey, l_shipdate, explode(lineitem_data) as lineitem_data from lineitem) as lineitem
where
	o_orderkey = l_orderkey
	and lineitem_data.l_shipmode in ('MAIL', 'SHIP')
	and lineitem_data.l_commitdate < lineitem_data.l_receiptdate
	and l_shipdate < lineitem_data.l_commitdate
	and lineitem_data.l_receiptdate >= date '1994-01-01'
	and lineitem_data.l_receiptdate < date '1994-01-01' + interval '1' year
group by
	lineitem_data.l_shipmode
order by
	lineitem_data.l_shipmode
