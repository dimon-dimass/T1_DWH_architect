insert into schema20.dds_regions(src_region_id, region_name, division_id, region_mgr_id, valid_from) 
select region_cd
	,region_name
	,(select division_id from schema20.dds_divisions where src_division_id = division_cd) division_id
	,region_mgr_associate_id
	,'2000-01-01'::date valid_from
	,'2999-12-31'::date valid_until
from schema20.stg_t_region 
union
select district_cd
	,district_name
	,(select division_id from schema20.dds_divisions where src_division_id = division_cd) division_id
	,region_mgr_associate_id
	,'2000-01-01'::date
	,'2999-12-31'::date
from schema20.stg_t_district;
