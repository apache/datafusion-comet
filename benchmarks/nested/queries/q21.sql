-- CometBench-H nested query 21 derived from TPC-H query 21 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- using default substitutions

select
	supplier_data.s_name,
	count(*) as numwait
from
	(select s_suppkey, s_nationkey, explode(supplier_data) as supplier_data from supplier) as supplier,
	(select l_orderkey, l_partkey, l_suppkey, l_shipdate, explode(lineitem_data) as lineitem_data from lineitem) as l1,
	(select o_orderkey, o_custkey, o_orderdate, explode(orders_data) as orders_data from orders) as orders,
	(select n_nationkey, n_regionkey, explode(nation_data) as nation_data from nation) as nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and orders_data.o_orderstatus = 'F'
	and l1.lineitem_data.l_receiptdate > l1.lineitem_data.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			(select l_orderkey, l_partkey, l_suppkey, l_shipdate, explode(lineitem_data) as lineitem_data from lineitem) as l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.lineitem_data.l_receiptdate > l3.lineitem_data.l_commitdate
	)
	and s_nationkey = n_nationkey
	and nation_data.n_name = 'SAUDI ARABIA'
group by
	supplier_data.s_name
order by
	numwait desc,
	supplier_data.s_name
limit 100