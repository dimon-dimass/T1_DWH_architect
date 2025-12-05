insert into schema20.dds_product_baskets
select *
	,null
from schema20.stg_osh_grocery_basket;