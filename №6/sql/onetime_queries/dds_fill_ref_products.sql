insert into schema20.dds_products
select item_id
	,item_name
	,item_type_cd
	,item_subclass_cd
	,null
	,brand_cd
	,vendor_party_id
	,commodity_cd
from schema20.stg_item;