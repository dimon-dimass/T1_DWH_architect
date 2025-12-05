insert into schema20.dds_divisions(src_division_id, division_name, country_id, division_mgr_id, valid_from) 
select division_cd
	,division_name
	,all_divisions_cd
	,division_mgr_associate_id
	,'2000-01-01'::date valid_from
	,'2999-12-31'::date valid_until
from schema20.stg_t_division;