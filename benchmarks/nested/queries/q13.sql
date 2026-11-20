-- CometBench-H nested query 13 derived from TPC-H query 13 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- using default substitutions

select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count
		from
			customer left outer join (select o_orderkey, o_custkey, o_orderdate, explode(orders_data) as orders_data from orders) as orders on
				c_custkey = o_custkey
				and orders_data.o_comment not like '%special%requests%'
		group by
			c_custkey
	) as c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc
