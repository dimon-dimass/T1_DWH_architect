insert into schema20.dds_location_inventory
select location_id
	,item_id
	,item_inv_time item_inv_time
	,on_hand_unit_qty
	,on_hand_at_retail_amt
	,on_hand_cost_amt
	,on_order_qty
	,lost_sales_day_ind
from schema20.stg_item_inventory
where item_inv_dt < '2023-12-01';