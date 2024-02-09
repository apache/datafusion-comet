select
        sum(o_custkey)
from
        orders
where
        o_orderpriority = '1-URGENT'
        or o_orderpriority = '2-HIGH'
group by
        o_orderkey
