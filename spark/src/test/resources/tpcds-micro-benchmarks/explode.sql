SELECT i_item_sk, explode(array(i_brand_id, i_class_id, i_category_id, i_manufact_id, i_manager_id))
FROM item
ORDER BY i_item_sk
LIMIT 1000