/*
	ЭТАП 1: ЗАГРУЗКА В ПРОМЕЖУТОЧНЫЙ (STAGGING) СЛОЙ
*/

select schema20.stg_fill_ref_tbls();

select schema20.stg_fill_dim_sales_transaction('2000-01-01', '2026-01-01');

select schema20.stg_fill_dim_sales_transaction_line('2000-01-01', '2026-01-01');

select schema20.stg_fill_dim_item_inventory('2000-01-01','2026-01-01');

/*
	ЭТАП 2: ЗАГРУЗКА ИСТОРИИ (РАЗОВАЯ) В ДЕТАЛЬНЫЙ СЛОЙ 
*/

-- Заполнение таблицы-справочника категорий (заполняем атрибут description нулем) 
insert into schema20.dds_categories 
select *, null 
from schema20.stg_osh_item_subclass;

-- Заполнение таблицы-справочника типов товаров (category_id и description заполняем нулем) 
insert into schema20.dds_product_types 
select *, null, null
from schema20.stg_osh_item_type;

-- Заполнение таблицы-справочника стран 
insert into schema20.dds_countries(country_id, country_name)
select *
from schema20.stg_t_all_divisions;

-- Заполнение таблицы-справочника подразделений 
insert into schema20.dds_divisions(src_division_id, division_name, country_id, division_mgr_id, valid_from) 
select division_cd
	,division_name
	,all_divisions_cd
	,division_mgr_associate_id
	,'2000-01-01'::date valid_from
	,'2999-12-31'::date valid_until
from schema20.stg_t_division;


-- Заполнение таблицы-справочника областей и регионов 
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

-- Заполнение таблицы-справочника точек/отделов 
insert into schema20.dds_locations(src_location_id, location_name, region_id, location_type, valid_from, valid_until)
select tloc.location_id 
	,tloc.location_name 
	,reg.region_id
	,loctp.location_type_desc	
	,tloc.location_open_dt
	,coalesce(tloc.location_effective_dt,'2999-12-31'::date) 
from schema20.stg_t_location tloc
left join schema20.dds_regions reg on tloc.district_cd = reg.src_region_id
left join schema20.stg_t_location_type loctp on tloc.location_type_cd = loctp.location_type_cd;

-- Заполнение таблицы-справочника товарных брендов 
insert into schema20.dds_product_brands
select brand_cd
	,brand_name
	,brand_party_id
from schema20.stg_t_brand;

-- Заполнение таблицы-справочника поставщиков/производителей (заполняем атрибут description нулем) 
insert into schema20.dds_product_manufactures 
select *
	,null
from schema20.stg_t_vendor;

-- Заполнение таблицы-справочника продуковых корзин 
insert into schema20.dds_product_baskets
select *
	,null
from schema20.stg_osh_grocery_basket;

-- Заполнение таблицы-справочника индексов премиальности 
insert into schema20.dds_product_commodities
select *
	,null
from schema20.stg_osh_commodity;


-- Заполнение таблицы-справочника товаров 
insert into schema20.dds_products
select item_id
	,item_name
	,item_type_cd
	,item_subclass_cd
	,null
	,brand_cd
	,vendor_party_id
	,commodity_cd
from schema20.stg_item;

-- Заполнение таблицы-справочника товаров 
insert into schema20.dds_customers(src_customer_id, gender_type_cd, credentials, city, valid_from)
select individual_party_id
	,gender_type_cd 
	,family_name || given_name || middle_name full_name 
	,city
	,'2000-01-01'::date
	,'2999-12-31'::date
from schema20.stg_customer;

-- Заполнение таблицы-фактов инвентаризации на точках 
insert into schema20.dds_location_inventory
select location_id
	,item_id
	,item_inv_time item_inv_time
	,on_hand_unit_qty
	,on_hand_at_retail_amt
	,on_hand_cost_amt
	,on_order_qty
	,lost_sales_day_ind
from schema20.stg_item_inventory
where item_inv_dt < '2023-12-01';

-- Заполнение таблицы-справочника причин изменения цен 
insert into schema20.dds_price_change_reasons
select *
from schema20.stg_t_price_change_reason;

-- Заполнение таблицы-справочника изменения цен товаров на точках
insert into schema20.dds_prod_inventory_price_history(product_id, department_id, unit_price, change_reason_cd, valid_from, valid_until)
select item_id
	,location_id 
	,item_price_amt 
	,price_change_reason_cd
	,COALESCE((select max(item_price_start_date) 
				from from schema20.stg_item_price_history iph2 
				where iph2.item_price_start_date < iph.item_price_start_date
					and iph2.location_id = iph.location_id 
					and iph2.item_id = iph.item_id), '2000-01-01') item_price_start_date
	,COALESCE((select min(item_price_start_dt)-interval'1 day' 
				from schema20.stg_item_price_history iph2 
				where iph2.item_price_start_dt > iph.item_price_start_dt 
					and iph2.location_id = iph.location_id 
					and iph2.item_id = iph.item_id),'2999-12-31') item_price_end_dt
from schema20.stg_item_price_history iph;

-- Заполнение таблицы-фактов чеков 
insert into schema20.dds_receipts(receipt_id, customer_id, employee_id, department_id, tran_type_cd, tran_status_cd, purchase_cost, purchase_qty
								,uniq_prods_qty, purchase_rev, tran_start_dttm, tran_end_dttm, purchase_date)
select sales_tran_id 
	,individual_party_id 
	,associate_party_id 
	,location_id 
	,tran_type_cd 
	,tran_status_cd 
	,mkb_cost_amt
	,mkb_item_qty
	,mkb_number_unique_items_qty
	,mkb_rev_amt
	,tran_start_dttm_dd 
	,tran_end_dttm_dd
	,tran_date 
from schema20.stg_sales_transaction
where tran_date < '2023-12-01';

-- Заполнение таблицы-фатов детализации чеков 
insert into schema20.dds_receipt_details
select sales_tran_id 
	,item_id 
	,avg(unit_selling_price_amt) unit_selling_price_amt
	,avg(unit_cost_amt) unit_cost_amt
	,sum(item_qty) item_qty
from schema20.stg_sales_transaction_line
where tran_line_date < '2023-12-01'
group by sales_tran_id, item_id;


/*
	ЭТАП 3: ЗАГРУЗКА ДАННЫХ ИЗ STAGGING В ДЕТАЛЬНЫЙ СЛОЙ С ПОМОЩЬЮ ФУНКЦИЙ
*/

select schema20.dds_fill_ref_categories();

select schema20.dds_fill_ref_product_types();

select schema20.dds_fill_ref_countries();

select schema20.dds_fill_ref_divisions();

select schema20.dds_fill_ref_regions();

select schema20.dds_fill_ref_locations();

select schema20.dds_fill_ref_product_brands();

select schema20.dds_fill_ref_manufactures();

select schema20.dds_fill_ref_product_buskets();

select schema20.dds_fill_ref_product_commodities();

select schema20.dds_fill_ref_products();

select schema20.dds_fill_ref_price_change_reasons();

select schema20.dds_fill_ref_prod_inventory_price_history();

select schema20.dds_fill_ref_customers();

select schema20.dds_fill_dim_location_inventory('2023-12-01');

select schema20.dds_fill_dim_receipts('2023-12-01');

select schema20.dds_fill_dim_receipt_details('2023-12-01');

/*
	ЭТАП 3-4: ЗАГРУЗКА(ОБНОВЛЕНИЕ) ДАННЫХ ВИТРИН 
*/

select schema20.dm_fill_sales_margin();

select scheam20.dm_fill_sales_matrix_2022_2023();

