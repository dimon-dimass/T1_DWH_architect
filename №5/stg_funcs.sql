/*
	ФУНКЦИИ ЗАГРУЗКИ ПРОМЕЖУТОЧНОГО СЛОЯ
*/

/*
	ОБЩАЯ ФУНКЦИЯ ДЛЯ ЗАПОЛНЕНИЯ ТАБЛИЦ-СПРАВОЧНИКОВ STG
*/

create or replace function schema20.stg_fill_ref_tbls()
returns VOID language sql volatile execute on any as $$
	-- Скрипт заполнения таблицы customer в подготовительном слое 
	
	-- ПЕРЕД НАПОЛНЕНИЕМ ВЫПОЛНЯЕТСЯ ОЧИСТА ТЕКУЩИХ ДАННЫХ ТАБЛИЦ-СПРАВОЧНИКОВ ПОДГОТОВИТЕЛЬНОГО СЛОЯ!!
	truncate table schema20.stg_customer;
	truncate table schema20.stg_item;
	truncate table schema20.stg_item_price_history;
	truncate table schema20.stg_osh_commodity;
	truncate table schema20.stg_osh_grocery_basket;
	truncate table schema20.stg_osh_item_subclass;
	truncate table schema20.stg_osh_item_type;
	truncate table schema20.stg_t_all_divisions;
	truncate table schema20.stg_t_brand;
	truncate table schema20.stg_t_channel;
	truncate table schema20.stg_t_district;
	truncate table schema20.stg_t_division;
	truncate table schema20.stg_t_location;
	truncate table schema20.stg_t_location_type;
	truncate table schema20.stg_t_price_change_reason;
	truncate table schema20.stg_t_region;
	truncate table schema20.stg_t_vendor;
	
	-- Скрипт заполнения таблицы customer в подготовительном слое 
	insert into schema20.stg_customer
	select individual_party_id
		,birth_dt
		,gender_type_cd 
		,given_name 
		,middle_name 
		,family_name
		,street_address
		,city
		,state_code
		,zip_code
	from schema20.src_jdbc_customer;
	
	-- Скрипт заполнения таблицы item в подготовительном слое 
	insert into schema20.stg_item
	select *
	from schema20.src_jdbc_item;
	
	-- Скрипт заполнения таблицы item_price_history в подготовительном слое 
	insert into schema20.stg_item_price_history
	select *
	from schema20.src_jdbc_item_price_history;
	
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
	select 	location_id
		,location_name
		,location_open_dt
		,location_effective_dt 
		,location_total_area_meas 
		,chain_cd
		,channel_cd
		,district_cd 
		,location_type_cd  
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
$$;

/*
	ФУНКЦИИ ЗАПОЛНЕНИЯ ТАБЛИЦ-ФАКТОВ STG СЛОЯ
*/

-- Функция заполнения stg_sales_transaction (один входной параметр: инкрементальная дата)
create or replace function schema20.stg_fill_dim_sales_transaction(in inc_dt date)
returns VOID language sql volatile execute on any as $$
	
	-- ПРЕДВАРИТЕЛЬНАЯ ОЧИСТА ТЕКУЩИХ ДАННЫХ ТАБЛИЦЫ stg_sales_transaction ПОДГОТОВИТЕЛЬНОГО СЛОЯ!!
	truncate table schema20.stg_sales_transaction;
	
	-- Скрипт заполнения таблицы sales_transaction в подготовительном слое 
	insert into schema20.stg_sales_transaction
	select * 
	from schema20.src_jdbc_sales_transaction
	where tran_date = inc_dt or date_trunc('day',tran_end_dttm_dd) = inc_dt;
$$;

-- Функция заполнения stg_sales_transaction (два входных параметра: начальная и конечная даты)
create or replace function schema20.stg_fill_dim_sales_transaction(in start_dt date, in end_dt date)
returns VOID language sql volatile execute on any as $$
	
	-- ПРЕДВАРИТЕЛЬНАЯ ОЧИСТА ТЕКУЩИХ ДАННЫХ ТАБЛИЦЫ stg_sales_transaction ПОДГОТОВИТЕЛЬНОГО СЛОЯ!!
	truncate table schema20.stg_sales_transaction;
	
	-- Скрипт заполнения таблицы sales_transaction в подготовительном слое 
	insert into schema20.stg_sales_transaction
	select * 
	from schema20.src_jdbc_sales_transaction
	where tran_date between start_dt and end_dt or date_trunc('day',tran_end_dttm_dd) between start_dt and end_dt;
$$;

-- Функция заполнения stg_sales_transaction_line (один входной параметр: инкрементальная дата)
create or replace function schema20.stg_fill_dim_sales_transaction_line(in inc_dt date)
returns VOID language sql volatile execute on any as $$

	-- ПРЕДВАРИТЕЛЬНАЯ ОЧИСТА ТЕКУЩИХ ДАННЫХ ТАБЛИЦЫ stg_sales_transaction_line ПОДГОТОВИТЕЛЬНОГО СЛОЯ!!
	truncate table schema20.stg_sales_transaction_line;

	-- Скрипт заполнения таблицы sales_transaction в подготовительном слое 
	insert into schema20.stg_sales_transaction_line
	select * 
	from schema20.src_jdbc_sales_transaction_line
	where tran_line_date = inc_dt;
$$;

-- Функция заполнения stg_sales_transaction_line (два входных параметра: начальная и конечная даты)
create or replace function schema20.stg_fill_dim_sales_transaction_line(in start_dt date, in end_dt date)
returns VOID language sql volatile execute on any as $$

	-- ПРЕДВАРИТЕЛЬНАЯ ОЧИСТА ТЕКУЩИХ ДАННЫХ ТАБЛИЦЫ stg_sales_transaction_line ПОДГОТОВИТЕЛЬНОГО СЛОЯ!!
	truncate table schema20.stg_sales_transaction_line;

	-- Скрипт заполнения таблицы sales_transaction в подготовительном слое 
	insert into schema20.stg_sales_transaction_line
	select * 
	from schema20.src_jdbc_sales_transaction_line
	where tran_line_date between start_dt and end_dt;
$$;

-- Функция заполнения stg_item_inventory (один входной параметр: инкрементальная дата)
create or replace function schema20.stg_fill_dim_item_inventory(in inc_dt date)
returns VOID language sql volatile execute on any as $$

	-- ПРЕДВАРИТЕЛЬНАЯ ОЧИСТА ТЕКУЩИХ ДАННЫХ ТАБЛИЦЫ stg_item_inventory ПОДГОТОВИТЕЛЬНОГО СЛОЯ!!
	truncate table schema20.stg_item_inventory;

	-- Скрипт заполнения таблицы item_inventory в подготовительном слое 
	insert into schema20.stg_item_inventory
	select *
	from schema20.src_jdbc_item_inventory
	where item_inv_dt = inc_dt;
$$;

-- Функция заполнения stg_item_inventory (два входных параметра: начальная и конечная даты)
create or replace function schema20.stg_fill_dim_item_inventory(in start_dt date, in end_dt date)
returns VOID language sql volatile execute on any as $$

	-- ПРЕДВАРИТЕЛЬНАЯ ОЧИСТА ТЕКУЩИХ ДАННЫХ ТАБЛИЦЫ stg_item_inventory ПОДГОТОВИТЕЛЬНОГО СЛОЯ!!
	truncate table schema20.stg_item_inventory;

	-- Скрипт заполнения таблицы item_inventory в подготовительном слое 
	insert into schema20.stg_item_inventory
	select *
	from schema20.src_jdbc_item_inventory
	where item_inv_dt between start_dt and end_dt;
$$;