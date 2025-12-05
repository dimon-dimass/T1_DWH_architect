/*
 Реализация разовой загрузки в детальный слой (данные не включают декабрь 2023 года) 
*/

do $$
declare 
	tbl record;
	schema_name TEXT = 'schema20';
begin
	for tbl in (
		select table_name
		from information_schema.tables
		where table_schema=schema_name and table_name like 'dds_%'
	) loop
		raise notice 'Таблица будет % удалена', tbl.table_name;
		execute format('drop table %I.%I cascade', schema_name, tbl.table_name);
	end loop;
end $$;


do $$
declare 
	tbl record;
	schema_name TEXT = 'schema20';
begin
	for tbl in (
		select table_name
		from information_schema.tables
		where table_schema=schema_name and table_name like 'stg_%'
	) loop
		raise notice 'Строки Таблицы будет % удалены', tbl.table_name;
		execute format('truncate table %I.%I', schema_name, tbl.table_name);
	end loop;
end $$;

-- Заполнение таблицы-справочника категорий (заполняем атрибут description нулем) SUCCESS
insert into schema20.dds_categories 
select *, null 
from schema20.stg_osh_item_subclass;

-- Заполнение таблицы-справочника типов товаров (category_id и description заполняем нулем) SUCCESS
insert into schema20.dds_product_types 
select *, null, null
from schema20.stg_osh_item_type;

-- Заполнение таблицы-справочника стран SUCCESS
insert into schema20.dds_countries(src_country_id, country_name, valid_from, valid_until)
select *
	,'1991-12-25'::date  
	,'2999-12-31'::date
from schema20.stg_t_all_divisions;

-- Заполнение таблицы-справочника подразделений SUCCESS
insert into schema20.dds_divisions(src_division_id, division_name, country_id, valid_from) 
select division_cd
	,division_name
	,all_divisions_cd 
	,'2000-01-01'::date valid_from
from schema20.stg_t_division;


-- Заполнение таблицы-справочника областей и регионов SUCCESS
insert into schema20.dds_regions(src_region_id, region_name, division_id, valid_from) 
select region_cd
	,region_name
	,floor(random()*((select max(division_id) from schema20.dds_divisions)-(select min(division_id) from schema20.dds_divisions)+1)+(select min(division_id) from schema20.dds_divisions)) division_id
	,'2000-01-01'::date valid_from
from schema20.stg_t_region 
union
select district_cd
	,district_name
	,floor(random()*((select max(division_id) from schema20.dds_divisions)-(select min(division_id) from schema20.dds_divisions)+1)+(select min(division_id) from schema20.dds_divisions)) division_id
	,'2000-01-01'::date
from schema20.stg_t_district;

-- Заполнение таблицы-справочника точек/отделов SUCCESS
insert into schema20.dds_departments(src_department_id, department_name, location_id, department_type, valid_from, valid_until)
select tloc.location_id 
	,tloc.location_name 
	,reg.region_id
	,loctp.location_type_desc	
	,tloc.location_open_dt
	,tloc.location_effective_dt 
from schema20.stg_t_location tloc
left join schema20.dds_regions reg on tloc.district_cd = reg.src_region_id
left join schema20.stg_t_location_type loctp on tloc.location_type_cd = loctp.location_type_cd;

-- Заполнение таблицы-справочника товарных брендов SUCCESS
insert into schema20.dds_product_brands
select brand_cd
	,brand_name
	,'Ответственный '||brand_party_id
from schema20.stg_t_brand;

-- Заполнение таблицы-справочника поставщиков/производителей (заполняем атрибут description нулем) SUCCESS
insert into schema20.dds_product_manufactures 
select *
	,null
from schema20.stg_t_vendor;

-- Заполнение таблицы-справочника продуковых корзин SUCCESS
insert into schema20.dds_product_baskets
select *
	,null
from schema20.stg_osh_grocery_basket;

-- Заполнение таблицы-справочника индексов премиальности SUCCESS
insert into schema20.dds_product_commodities
select *
	,null
from schema20.stg_osh_commodity;


-- Заполнение таблицы-справочника товаров SUCCESS
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

-- Заполнение таблицы-фактов инвентаризации на точках SUCCESS
insert into schema20.dds_department_inventory
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

-- Заполнение таблицы-справочника причин изменения цен SUCCESS
insert into schema20.dds_price_change_reasons
select *
from schema20.stg_t_price_change_reason;

-- Заполнение таблицы-справочника изменения цен товаров на точках
insert into schema20.dds_prod_inventory_price_history(product_id, department_id, unit_price, change_reason_cd, valid_from, valid_until)
select item_id
	,location_id 
	,item_price_amt 
	,price_change_reason_cd
	,item_price_start_dt
	,CASE when current_indicator = 'N' then (select min(item_price_start_dt) from schema20.stg_item_price_history iph2 
													where iph2.item_price_start_dt > iph.item_price_start_dt 
														and iph2.location_id = iph.location_id 
														and iph2.item_id = iph.item_id)
		else null END
from schema20.stg_item_price_history iph;

truncate table schema20.dds_prod_inventory_price_history;

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
insert into schema20.dds_receipt_details()
select sales_tran_id 
	,item_id 
	,unit_selling_price_amt
	,unit_cost_amt
	,item_qty
from schema20.stg_sales_transaction_line
where tran_line_date < '2023-12-01';






