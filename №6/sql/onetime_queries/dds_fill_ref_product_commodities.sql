insert into schema20.dds_product_commodities
select *
	,null
from schema20.stg_osh_commodity;