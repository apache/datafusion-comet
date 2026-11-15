-- CometBench-H nested query 11 derived from TPC-H query 11 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- using default substitutions

select
	ps_partkey,
	sum(partsupp_data.ps_supplycost * partsupp_data.ps_availqty) as value
from
	(select ps_partkey, ps_suppkey, explode(partsupp_data) as partsupp_data from partsupp) as partsupp,
	supplier,
	(select n_nationkey, n_regionkey, explode(nation_data) as nation_data from nation) as nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and nation_data.n_name = 'GERMANY'
group by
	ps_partkey having
		sum(partsupp_data.ps_supplycost * partsupp_data.ps_availqty) > (
			select
				sum(partsupp_data.ps_supplycost * partsupp_data.ps_availqty) * 0.0001000000
			from
				(select ps_partkey, ps_suppkey, explode(partsupp_data) as partsupp_data from partsupp) as partsupp,
				supplier,
				(select n_nationkey, n_regionkey, explode(nation_data) as nation_data from nation) as nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and nation_data.n_name = 'GERMANY'
		)
order by
	value desc
