insert into schema20.dds_product_brands
select brand_cd
	,brand_name
	,brand_party_id
from schema20.stg_t_brand;
