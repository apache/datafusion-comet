-- This query filters both inputs and was created to measure the overhead of both CopyExec and FilterExec
-- performing copies of arrays

select catalog_sales.*, item.*
from catalog_sales
join item on (cs_item_sk = i_item_sk)
where cs_item_sk % 2 == 0 and cs_item_sk < 10000
and i_item_sk % 2 == 0 and i_item_sk < 10000
