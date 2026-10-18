-- CometBench-H nested query 22 derived from TPC-H query 22 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- using default substitutions

select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(customer_data.c_phone, 1, 2) as cntrycode,
			customer_data.c_acctbal as c_acctbal
		from
			(select c_custkey, c_nationkey, c_mktsegment, explode(customer_data) as customer_data from customer) as customer
		where
			substring(customer_data.c_phone, 1, 2) in
				('13', '31', '23', '29', '30', '18', '17')
			and customer_data.c_acctbal > (
				select
					avg(customer_data.c_acctbal)
				from
					(select c_custkey, c_nationkey, c_mktsegment, explode(customer_data) as customer_data from customer) as customer
				where
					customer_data.c_acctbal > 0.00
					and substring(customer_data.c_phone, 1, 2) in
						('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode
