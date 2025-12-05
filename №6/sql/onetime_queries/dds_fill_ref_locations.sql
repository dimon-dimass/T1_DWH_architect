insert into schema20.dds_departments(src_department_id, department_name, location_id, department_type, valid_from, valid_until)
select tloc.location_id 
	,tloc.location_name 
	,reg.region_id
	,loctp.location_type_desc	
	,tloc.location_open_dt
	,coalesce(tloc.location_effective_dt,'2999-12-31'::date) 
from schema20.stg_t_location tloc
left join schema20.dds_regions reg on tloc.district_cd = reg.src_region_id
left join schema20.stg_t_location_type loctp on tloc.location_type_cd = loctp.location_type_cd;
