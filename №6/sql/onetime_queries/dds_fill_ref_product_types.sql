insert into schema20.dds_product_types 
select *, null, null
from schema20.stg_osh_item_type;
