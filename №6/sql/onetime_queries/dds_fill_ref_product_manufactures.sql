insert into schema20.dds_product_manufactures 
select *
	,null
from schema20.stg_t_vendor;
