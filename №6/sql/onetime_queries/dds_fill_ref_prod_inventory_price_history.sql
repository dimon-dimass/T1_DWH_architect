insert into schema20.dds_prod_inventory_price_history(product_id, department_id, unit_price, change_reason_cd, valid_from, valid_until)
select item_id
	,location_id 
	,item_price_amt 
	,price_change_reason_cd
	,COALESCE((select max(item_price_start_date) 
				from from schema20.stg_item_price_history iph2 
				where iph2.item_price_start_date < iph.item_price_start_date
					and iph2.location_id = iph.location_id 
					and iph2.item_id = iph.item_id), '2000-01-01') item_price_start_date
	,COALESCE((select min(item_price_start_dt)-interval'1 day' 
				from schema20.stg_item_price_history iph2 
				where iph2.item_price_start_dt > iph.item_price_start_dt 
					and iph2.location_id = iph.location_id 
					and iph2.item_id = iph.item_id),'2999-12-31') item_price_end_dt
from schema20.stg_item_price_history iph;