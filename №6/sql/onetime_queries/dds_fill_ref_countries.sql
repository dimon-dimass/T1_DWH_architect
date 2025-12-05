insert into schema20.dds_countries(country_id, country_name)
select *
from schema20.stg_t_all_divisions;