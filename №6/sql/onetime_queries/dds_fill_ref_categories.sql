insert into schema20.dds_categories 
select *, null 
from schema20.stg_osh_item_subclass;