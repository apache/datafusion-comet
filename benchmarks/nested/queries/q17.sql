-- CometBench-H nested query 17 derived from TPC-H query 17 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- using default substitutions

select
	sum(lineitem_data.l_extendedprice) / 7.0 as avg_yearly
from
	(select l_orderkey, l_partkey, l_suppkey, l_shipdate, explode(lineitem_data) as lineitem_data from lineitem) as lineitem,
	(select p_partkey, p_brand, explode(part_data) as part_data from part) as part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#23'
	and part_data.p_container = 'MED BOX'
	and lineitem_data.l_quantity < (
		select
			0.2 * avg(lineitem_data.l_quantity)
		from
			(select l_orderkey, l_partkey, l_suppkey, l_shipdate, explode(lineitem_data) as lineitem_data from lineitem) as lineitem
		where
			l_partkey = p_partkey
	)
