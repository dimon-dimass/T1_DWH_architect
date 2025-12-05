insert into schema20.dds_customers(src_customer_id, gender_type_cd, credentials, city, valid_from)
select individual_party_id
	,gender_type_cd 
	,family_name || given_name || middle_name full_name 
	,city
	,'2000-01-01'::date
	,'2999-12-31'::date
from schema20.stg_customer;