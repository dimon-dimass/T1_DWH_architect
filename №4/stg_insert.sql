/*
 * СКРИПТЫ ЗАПОЛНЕНИЯ И ОЧИСТКИ ОТ ПУСТЫХ СТОЛБЦОВ
 */

-- Скрипт заполнения таблицы customer в подготовительном слое 
insert into schema20.stg_customer
select *
from schema20.src_jdbc_customer;

-- Скрипт заполнения таблицы item в подготовительном слое 
insert into schema20.stg_item
select *
from schema20.src_jdbc_item;

-- Скрипт заполнения таблицы item_inventory в подготовительном слое 
insert into schema20.stg_item_inventory
select *
from schema20.src_jdbc_item_inventory;

-- Скрипт заполнения таблицы item_price_history в подготовительном слое 
insert into schema20.stg_item_price_history
select *
from schema20.src_jdbc_item_price_history;

-- Скрипт заполнения таблицы sales_transaction в подготовительном слое 
insert into schema20.stg_sales_transaction
select * 
from schema20.src_jdbc_sales_transaction;

-- Скрипт заполнения таблицы sales_transaction_line в подготовительном слое 
insert into schema20.stg_sales_transaction_line
select *
from schema20.src_jdbc_sales_transaction_line;

-- Скрипт заполнения таблицы osh_commodity в подготовительном слое 
insert into schema20.stg_osh_commodity
select *
from schema20.src_gpfdist_osh_commodity;

-- Скрипт заполнения таблицы osh_grocery_basket в подготовительном слое 
insert into schema20.stg_osh_grocery_basket
select *
from schema20.src_gpfdist_osh_grocery_basket;

-- Скрипт заполнения таблицы osh_item_subclass в подготовительном слое 
insert into schema20.stg_osh_item_subclass
select *
from schema20.src_gpfdist_osh_item_subclass;

-- Скрипт заполнения таблицы osh_item_type в подготовительном слое 
insert into schema20.stg_osh_item_type
select *
from schema20.src_gpfdist_osh_item_type;

-- Скрипт заполнения таблицы t_all_divisions в подготовительном слое 
insert into schema20.stg_t_all_divisions
select *
from schema20.src_gpfdist_t_all_divisions;

-- Скрипт заполнения таблицы t_brand в подготовительном слое 
insert into schema20.stg_t_brand
select 	*
from schema20.src_gpfdist_t_brand;

-- Скрипт заполнения таблицы t_channel в подготовительном слое 
insert into schema20.stg_t_channel
select *
from schema20.src_gpfdist_t_channel;

-- Скрипт заполнения таблицы t_district в подготовительном слое 
insert into schema20.stg_t_district
select *
from schema20.src_gpfdist_t_district;

-- Скрипт заполнения таблицы t_division в подготовительном слое 
insert into schema20.stg_t_division
select * 
from schema20.src_gpfdist_t_division;

-- Скрипт заполнения таблицы t_location в подготовительном слое 
insert into schema20.stg_t_location
select *
from schema20.src_gpfdist_t_location;

-- Скрипт заполнения таблицы t_location_type в подготовительном слое 
insert into schema20.stg_t_location_type
select *
from schema20.src_gpfdist_t_location_type;

-- Скрипт заполнения таблицы t_price_change_reason в подготовительном слое 
insert into schema20.stg_t_price_change_reason
select *
from schema20.src_gpfdist_t_price_change_reason;

-- Скрипт заполнения таблицы t_region в подготовительном слое 
insert into schema20.stg_t_region
select 	*
from schema20.src_gpfdist_t_region;

-- Скрипт заполнения таблицы t_vendor в подготовительном слое 
insert into schema20.stg_t_vendor
select *
from schema20.src_gpfdist_t_vendor;


