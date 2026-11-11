-- CometBench-H nested query 15 derived from TPC-H query 15 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- using default substitutions

with revenue0 as
	(select
		l_suppkey as supplier_no,
		sum(lineitem_data.l_extendedprice * (1 - lineitem_data.l_discount)) as total_revenue
	from
		(select l_orderkey, l_partkey, l_suppkey, l_shipdate, explode(lineitem_data) as lineitem_data from lineitem) as lineitem
	where
		l_shipdate >= date '1996-01-01'
		and l_shipdate < date '1996-01-01' + interval '3' month
	group by
		l_suppkey)


select
	s_suppkey,
	supplier_data.s_name,
	supplier_data.s_address,
	supplier_data.s_phone,
	total_revenue
from
	(select s_suppkey, s_nationkey, explode(supplier_data) as supplier_data from supplier) as supplier,
	revenue0
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue0
	)
order by
	s_suppkey

