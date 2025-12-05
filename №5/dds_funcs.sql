/*
	ФУНКЦИИ ЗАГРУЗКИ ТАБЛИЦ-СПРАВОЧНИКОВ
	
	ДЛЯ ТАБЛИЦ SCD TYPE 0,1 ДОБАВЛЯЮТСЯ ТОЛЬКО НОВЫЕ СТРОКИ, ДЛЯ ТАБЛИЦ SCD TYPE 2 РЕАЛИЗУЕТСЯ ОБНОВЛЕНИЕ СУЩЕСТВУЮЩЕЙ СТРОКИ И ДОБАВЛЕНИЕ НОВОЙ
*/

-- Функция заполнения dds_categories
create or replace function schema20.dds_fill_ref_categories()
returns VOID language sql volatile execute on any as $$

	-- Заполнение таблицы-справочника категорий (заполняем атрибут description нулем) 
	insert into schema20.dds_categories (category_id, category_name, description)
	select item_subclass_cd, item_subclass_name, null dsc
	from schema20.stg_osh_item_subclass ois
	where not exists (
		select 1
		from schema20.dds_categories ctg
		where ctg.category_id = ois.item_subclass_cd
	);

	-- Обновление атрибутов dds_categories 
	update schema20.dds_categories c
	set category_name = ois.item_subclass_name
	from schema20.stg_osh_item_subclass ois
	where c.category_id = ois.item_subclass_cd;

--	delete from schema20.stg_osh_item_subclass;
$$;

-- Функция заполнения dds_product_types
create or replace function schema20.dds_fill_ref_product_types()
returns VOID language sql volatile execute on any as $$

	-- Заполнение таблицы-справочника типов товаров (category_id и description заполняем нулем)
	insert into schema20.dds_product_types 
	select *, null, null
	from schema20.stg_osh_item_type itp
	where not exists (
		select 1
		from schema20.dds_product_types pt
		where pt.product_type_id = itp.item_type_cd
	);

	-- Обновление атрибутов dds_product_types 
	update schema20.dds_product_types pt
	set product_type_name = oit.item_type_name 
	from schema20.stg_osh_item_type oit
	where pt.product_type_id = oit.item_type_cd;

--	delete from schema20.stg_osh_item_type;
$$;

-- Функция заполнения dds_countries
create or replace function schema20.dds_fill_ref_countries()
returns VOID language sql volatile execute on any as $$

	-- Заполнение таблицы-справочника типов товаров (category_id и description заполняем нулем)
	insert into schema20.dds_countries 
	select *
	from schema20.stg_t_all_divisions tad
	where not exists (
		select 1
		from schema20.dds_countries c
		where c.country_id = tad.all_divisions_cd
	);

	-- Обновление атрибутов dds_countries 
	update schema20.dds_countries c
	set country_name = tad.all_divisions_name 
	from schema20.stg_t_all_divisions tad
	where c.country_id = tad.all_divisions_cd;

--	delete from schema20.stg_t_all_divisions;
end
$$;

-- Функция заполнения dds_divisions
create or replace function schema20.dds_fill_ref_divisions()
returns VOID language plpgsql volatile execute on any as $$
declare
	r RECORD;
begin	-- Заполнение таблицы-справочника подразделений
	FOR r IN (
	select division_cd
		,division_name
		,all_divisions_cd 
		,division_mgr_associate_id
		,CURRENT_DATE-1 valid_from
	from schema20.stg_t_division)
	LOOP
		PERFORM 1
		FROM schema20.dds_divisions
		WHERE src_division_id = r.division_cd
		AND division_name = r.division_name
		AND division_mgr_id = r.division_mgr_associate_id;

		IF NOT FOUND THEN
			IF EXISTS(
				SELECT 1
				FROM schema20.dds_divisions
				WHERE src_division_id = r.division_cd
			) THEN
				UPDATE schema20.dds_divisions
				SET valid_until = r.valid_from
				WHERE src_division_id = r.division_cd AND valid_until is null;
			END IF;

			insert into schema20.dds_divisions(src_division_id, division_name, country_id, division_mgr_id, valid_from, valid_until)
			values (r.division_cd, r.division_name, r.all_divisions_cd, r.division_mgr_associate_id, r.valid_from+1, '2999-12-31');
		END IF;
	END LOOP;

--	delete from schema20.stg_t_division;
end
$$;

-- Функция заполнения dds_regions
create or replace function schema20.dds_fill_ref_regions()
returns VOID language plpgsql volatile execute on any as $$

declare
	r RECORD;
begin	-- Заполнение таблицы-справочника областей/регионов
	FOR r IN (
		select region_cd
			,region_name
			,(select division_id from schema20.dds_divisions where src_division_id = division_cd) division_id
			,CURRENT_DATE-1 valid_from
		from schema20.stg_t_region 
		union
		select district_cd
			,district_name
			,(select division_id from schema20.dds_divisions where src_division_id = division_cd) division_id
			,CURRENT_DATE-1
		from schema20.stg_t_district
	)
	LOOP
		PERFORM 1
		FROM schema20.dds_regions
		WHERE src_region_id = r.region_cd
		AND region_name = r.region_name
		AND region_mgr_id = r.region_mgr_associate_id;
		
		IF NOT FOUND THEN
			IF EXISTS(
				SELECT 1
				FROM schema20.dds_regions
				WHERE src_region_id = r.region_cd
			) THEN
				UPDATE schema20.dds_regions
				SET valid_until = r.valid_from
				WHERE src_region_id = r.region_cd AND valid_until is null;
			END IF;

			insert into schema20.dds_regions(src_region_id, region_name, division_id, region_mgr_id, valid_from, valid_until)
			values (r.region_cd, r.region_name, r.division_cd, r.region_mgr_associate_id, r.valid_from+1, '2999-12-31');
		END IF;
	END LOOP;

--	delete from schema20.stg_t_region;
--	delete from schema20.stg_t_district;

end
$$;

-- Функция заполнения dds_departments
create or replace function schema20.dds_fill_ref_locations()
returns VOID language plpgsql volatile execute on any as $$

declare
	r RECORD;
begin	-- Заполнение таблицы-справочника отделов/точек компании
	FOR r IN (
		select tloc.location_id location_id
			,tloc.location_name  location_name
			,reg.region_id region_id
			,loctp.location_type_desc location_type_desc
			,tloc.location_open_dt location_open_dt
			,tloc.location_effective_dt location_effective_dt
		from schema20.stg_t_location tloc
		left join schema20.dds_regions reg on tloc.district_cd = reg.src_region_id
		left join schema20.stg_t_location_type loctp on tloc.location_type_cd = loctp.location_type_cd
	)
	LOOP
		PERFORM 1
		FROM schema20.dds_departments
		WHERE src_department_id = r.location_id
		AND department_name = r.location_name
		AND location_id = r.region_id
		AND department_type = r.location_type_desc;
		
		IF NOT FOUND THEN
			IF EXISTS(
				SELECT 1
				FROM schema20.dds_departments
				WHERE src_department_id = r.location_id
			) THEN
				UPDATE schema20.dds_departments
				SET valid_until = CURRENT_DATE
				WHERE src_department_id = r.location_id AND valid_until is null;
			END IF;

			insert into schema20.dds_departments(src_department_id, department_name, location_id, department_type, valid_from, valid_until)
			values (r.location_id, r.location_name, r.region_id, r.location_type_desc, r.location_open_dt, r.location_effective_dt);
		END IF;
	END LOOP;

--	delete from schema20.stg_t_location;
--	delete from schema20.stg_t_location_type;
end $$;

-- Функция заполнения dds_product_brands
create or replace function schema20.dds_fill_ref_product_brands()
returns VOID language sql volatile execute on any as $$
	
	-- Заполнение таблицы-справочника товарных брендов 
	insert into schema20.dds_product_brands
	select brand_cd
		,brand_name
		,brand_party_id
	from schema20.stg_t_brand tbr
	where not exists (
		select 1
		from schema20.dds_product_brands br
		where br.brand_id = tbr.brand_cd
	);

	-- Обновление атрибутов dds_product_brands 
	update schema20.dds_product_brands pb
	set brand_name = tb.brand_name 
	from schema20.stg_t_brand tb
	where pb.brand_id = tb.brand_cd;

--	delete from schema20.stg_t_brand;
$$;

-- Функция заполнения dds_manufactures
create or replace function schema20.dds_fill_ref_manufactures()
returns VOID language sql volatile execute on any as $$

	-- Заполнение таблицы-справочника поставщиков/производителей (заполняем атрибут description нулем) 
	insert into schema20.dds_product_manufactures 
	select *
		,null
	from schema20.stg_t_vendor v
	where not exists (
		select 1
		from schema20.dds_product_manufactures mf
		where mf.manufacture_id = v.vendor_party_id
	);

	-- Обновление атрибутов dds_product_manufactures 
	update schema20.dds_product_manufactures pm
	set manufacture_name = tv.vendor_name 
	from schema20.stg_t_vendor tv
	where pm.manufacture_id = tv.vendor_party_id;

--	delete from schema20.stg_t_vendor;
$$;

-- Функция заполнения dds_product_baskets
create or replace function schema20.dds_fill_ref_product_buskets()
returns VOID language sql volatile execute on any as $$

	-- Заполнение таблицы-справочника продуковых корзин 
	insert into schema20.dds_product_baskets
	select *
		,null
	from schema20.stg_osh_grocery_basket gb
	where not exists (
		select 1
		from schema20.dds_product_baskets pb
		where pb.basket_id = gb.grocery_basket_cd
	);

	-- Обновление атрибутов dds_product_baskets 
	update schema20.dds_product_baskets pb
	set basket_name = gb.grocery_basket_desc 
	from schema20.stg_osh_grocery_basket gb
	where pb.basket_id = gb.grocery_basket_cd;

--	delete from schema20.stg_osh_grocery_basket;
$$;

-- Функция заполнения dds_product_commodities
create or replace function schema20.dds_fill_ref_product_commodities()
returns VOID language sql volatile execute on any as $$

	-- Заполнение таблицы-справочника индексов премиальности 
	insert into schema20.dds_product_commodities
	select *
		,null
	from schema20.stg_osh_commodity c
	where not exists (
		select 1
		from schema20.dds_product_commodities pc
		where pc.commodity_id = c.commodity_cd
	);

	-- Обновление атрибутов dds_product_commodities 
	update schema20.dds_product_commodities pc
	set commodity_name = c.commodity_name, basket_id = c.grocery_basket_cd
	from schema20.stg_osh_commodity c
	where pc.commodity_id = c.commodity_cd;

--	delete from schema20.stg_osh_commodity;
$$;

-- Функция заполнения dds_products
create or replace function schema20.dds_fill_ref_products()
returns VOID language sql volatile execute on any as $$

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
	from schema20.stg_item i
	where not exists (
		select 1
		from schema20.dds_products p
		where p.product_id = i.item_id
	);

	-- Обновление атрибутов dds_products 
	update schema20.dds_products p
	set product_name = i.item_name
		,product_type_id = i.item_type_cd
		,product_category_id = i.item_subclass_cd
		,brand_id = i.brand_cd
		,manufacturer_id = i.vendor_party_id
		,commodity_id = i.commodity_cd
	from schema20.stg_item i
	where p.product_id = i.item_id;

--	delete from schema20.stg_item;
$$;

-- Функция заполнения dds_price_change_reasons
create or replace function schema20.dds_fill_ref_price_change_reasons()
returns VOID language sql volatile execute on any as $$

	-- Заполнение таблицы-справочника причин изменения цен SUCCESS
	insert into schema20.dds_price_change_reasons
	select *
	from schema20.stg_t_price_change_reason tpcr
	where not exists (
		select 1
		from schema20.dds_price_change_reasons pcr
		where pcr.change_reason_cd = tpcr.price_change_reason_cd
	);

	-- Обновление атрибутов dds_price_change_reasons 
	update schema20.dds_price_change_reasons pcr
	set change_desc = tpcr.price_change_reason_desc
	from schema20.stg_t_price_change_reason tpcr
	where pcr.change_reason_cd = tpcr.price_change_reason_cd;

--	delete from schema20.stg_t_price_change_reason;
$$;

-- Функция заполнения dds_prod_inventory_price_history
create or replace function schema20.dds_fill_ref_prod_inventory_price_history()
returns VOID language plpgsql volatile execute on any as $$

declare
	r RECORD;
begin	-- Заполнение таблицы-справочника изменчивости цены товаров на точках
	FOR r IN (
		select item_id
			,location_id 
			,item_price_amt 
			,price_change_reason_cd
			,COALESCE((select max(item_price_start_date) 
						from schema20.stg_item_price_history iph2 
						where iph2.item_price_start_date < iph.item_price_start_date
							and iph2.location_id = iph.location_id 
							and iph2.item_id = iph.item_id), '2000-01-01') item_price_start_date
			,COALESCE((select min(item_price_start_dt)-interval'1 day' 
						from schema20.stg_item_price_history iph2 
						where iph2.item_price_start_dt > iph.item_price_start_dt 
							and iph2.location_id = iph.location_id 
							and iph2.item_id = iph.item_id),'2999-12-31') item_price_end_dt
		from schema20.stg_item_price_history iph
	)
	LOOP
		PERFORM 1
		FROM schema20.dds_prod_inventory_price_history
		WHERE product_id = r.item_id
		AND department_id = r.location_id
		AND unit_price = r.item_price_amt
		AND change_reason_cd = r.price_change_reason_cd
		AND valid_from = r.item_price_start_dt;
		
		IF NOT FOUND THEN
			IF EXISTS(
				SELECT 1
				FROM schema20.dds_prod_inventory_price_history
				WHERE product_id = r.item_id
					AND department_id = r.location_id
			) THEN
				UPDATE schema20.dds_prod_inventory_price_history
				SET valid_until = r.item_price_start_date-1
				WHERE product_id = r.item_id AND department_id = r.location_id AND valid_until is null;
			END IF;

			insert into schema20.dds_prod_inventory_price_history(product_id, department_id, unit_price, change_reason_cd, valid_from, valid_until)
			values (r.item_id, r.location_id, r.item_price_amt, r.price_change_reason_cd, r.item_price_start_dt, r.item_price_end_dt);
		END IF;
	END LOOP;

--	delete from schema20.stg_item_price_history;
end $$;

-- Функция заполнения dds_customers
create or replace function schema20.dds_fill_ref_customers()
returns VOID language plpgsql volatile execute on any as $$

declare
	r RECORD;
begin	-- Заполнение таблицы-справочника изменчивости цены товаров на точках
	FOR r IN (
		select individual_party_id
			,gender_type_cd 
			,family_name || given_name || middle_name full_name 
			,city
		from schema20.stg_customer
	)
	LOOP
		PERFORM 1
		FROM schema20.dds_customers
		WHERE src_customer_id = r.individual_party_id
		AND gender_type_cd = r.gender_type_cd
		AND credetials = r.full_name
		AND city = r.city;
		
		IF NOT FOUND THEN
			IF EXISTS(
				SELECT 1
				FROM schema20.dds_customers
				WHERE src_customer_id = r.individual_party_id
			) THEN
				UPDATE schema20.dds_customers
				SET valid_until = CURRENT_DATE-1
				WHERE src_customer_id = r.individual_party_id AND valid_until is null;
			END IF;

			insert into schema20.dds_customer(src_customer_id, gender_type_cd, credetials, city, valid_from, valid_until)
			values (r.individual_party_id, r.location_id, r.full_name, r.city, CURRENT_DATE, null);
		END IF;
	END LOOP;

--	delete from schema20.stg_customer;
end $$;


/*
	ФУНКЦИИ ЗАПОЛНЕНИЯ ТАБЛИЦ-ФАКТОВ (ИНКРЕМЕНТАЛЬНЫЕ)
*/


-- Функция заполнения dds_department_inventory (один входной параметр: инкрементальная дата)
create or replace function schema20.dds_fill_dim_location_inventory(in inc_dt date)
returns VOID language sql volatile execute on any as $$

	-- Заполнение таблицы-фактов инвентаризации на точках
	insert into schema20.dds_department_inventory
	select location_id
		,item_id
		,item_inv_time
		,on_hand_unit_qty
		,on_hand_at_retail_amt
		,on_hand_cost_amt
		,on_order_qty
		,lost_sales_day_ind
	from schema20.stg_item_inventory ii
	where item_inv_dt = inc_dt and not exists (
		select 1
		from schema20.dds_department_inventory di
		where di.department_id = ii.location_id
			and di.product_id = ii.item_id
			and di.inventory_datetime = ii.item_inv_time
	);

	-- Обновление атрибутов dds_department_inventory 
	update schema20.dds_department_inventory di
	set stock_qty = ii.on_hand_unit_qty
		,stock_retail_price = ii.on_hand_at_retail_amt
		,stock_cost_price = ii.on_hand_cost_amt
		,on_order_qty = ii.on_order_qty
		,lost_sales_day_ind = ii.lost_sales_day_ind
	from schema20.stg_item_inventory ii
	where di.department_id = ii.location_id
			and di.product_id = ii.item_id
			and di.inventory_datetime = ii.item_inv_time;

--	delete from schema20.stg_item_inventory;

$$;

-- Функция заполнения dds_department_inventory (два входных параметра: начальная и конечная даты)
create or replace function schema20.dds_fill_dim_location_inventory(in start_dt date, in end_dt date)
returns VOID language sql volatile execute on any as $$

	-- Заполнение таблицы-фактов инвентаризации на точках
	insert into schema20.dds_department_inventory
	select location_id
		,item_id
		,item_inv_time
		,on_hand_unit_qty
		,on_hand_at_retail_amt
		,on_hand_cost_amt
		,on_order_qty
		,lost_sales_day_ind
	from schema20.stg_item_inventory ii
	where item_inv_dt between start_dt and end_dt and not exists (
		select 1
		from schema20.dds_department_inventory di
		where di.department_id = ii.location_id
			and di.product_id = ii.item_id
			and di.inventory_datetime = ii.item_inv_time
	);

	-- Обновление атрибутов dds_department_inventory 
	update schema20.dds_department_inventory di
	set stock_qty = ii.on_hand_unit_qty
		,stock_retail_price = ii.on_hand_at_retail_amt
		,stock_cost_price = ii.on_hand_cost_amt
		,on_order_qty = ii.on_order_qty
		,lost_sales_day_ind = ii.lost_sales_day_ind
	from schema20.stg_item_inventory ii
	where di.department_id = ii.location_id
			and di.product_id = ii.item_id
			and di.inventory_datetime = ii.item_inv_time;

--	delete from schema20.stg_item_inventory;

$$;

-- Функция заполнения dds_receipts (один входной параметр: инкрементальная дата)
create or replace function schema20.dds_fill_dim_receipts(in inc_dt date)
returns VOID language plpgsql volatile execute on any as $$

declare
	r RECORD;
begin	-- Заполнение таблицы-справочника изменчивости цены товаров на точках
	FOR r IN (
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
		where tran_date = inc_dt or date_trunc('day', tran_end_dttm_dd) = inc_dt
	)
	LOOP
		PERFORM 1
		FROM schema20.dds_receipts
		WHERE receipt_id = r.sales_tran_id
		AND customer_id = r.individual_party_id
		AND employee_id = r.associate_party_id
		AND department_id = r.location_id
		AND tran_status_cd = r.tran_status_cd
		AND tran_end_dttm = r.tran_end_dttm_dd;
		
		IF NOT FOUND THEN
			IF EXISTS(
				SELECT 1
				FROM schema20.dds_receipts
				WHERE receipt_id = r.sales_tran_id
			) THEN
				UPDATE schema20.dds_receipts
				SET tran_end_dttm = CURRENT_TIMESTAMP, tran_status_cd = r.tran_status_cd
				WHERE receipt_id = r.sales_tran_id AND customer_id = r.individual_party_id AND employee_id = r.associate_party_id
					AND department_id = r.location_id AND tran_end_dttm is null;
			END IF;

			insert into schema20.dds_receipts(receipt_id, customer_id, employee_id, department_id, tran_type_cd, tran_status_cd, purchase_cost, purchase_qty
								,uniq_prods_qty, purchase_rev, tran_start_dttm, tran_end_dttm, purchase_date)
			values (r.sales_tran_id, r.individual_party_id, r.associate_party_id, r.location_id, r.tran_type_cd, r.tran_status_cd, r.mkb_cost_amt, r.mkb_item_qty
				,r.mkb_number_unique_items_qty, r.mkb_rev_amt, r.tran_start_dttm_dd, r.tran_end_dttm_dd, r.tran_date);
		END IF;
	END LOOP;

--	delete from schema20.stg_sales_transaction;
end $$;

-- Функция заполнения dds_receipts (два входных параметра: начальная и конечная даты)
create or replace function schema20.dds_fill_dim_receipts(in start_dt date, in end_dt date)
returns VOID language plpgsql volatile execute on any as $$

declare
	r RECORD;
begin	-- Заполнение таблицы-справочника изменчивости цены товаров на точках
	FOR r IN (
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
		where tran_date between start_dt and end_dt or date_trunc('day', tran_end_dttm_dd) between start_dt and end_dt
	)
	LOOP
		PERFORM 1
		FROM schema20.dds_receipts
		WHERE receipt_id = r.sales_tran_id
		AND customer_id = r.individual_party_id
		AND employee_id = r.associate_party_id
		AND department_id = r.location_id
		AND tran_status_cd = r.tran_status_cd
		AND tran_end_dttm = r.tran_end_dttm_dd;
		
		IF NOT FOUND THEN
			IF EXISTS(
				SELECT 1
				FROM schema20.dds_receipts
				WHERE receipt_id = r.sales_tran_id
			) THEN
				UPDATE schema20.dds_receipts
				SET tran_end_dttm = CURRENT_TIMESTAMP, tran_status_cd = r.tran_status_cd
				WHERE receipt_id = r.sales_tran_id AND customer_id = r.individual_party_id AND employee_id = r.associate_party_id
					AND department_id = r.location_id AND tran_end_dttm is null;
			END IF;

			insert into schema20.dds_receipts(receipt_id, customer_id, employee_id, department_id, tran_type_cd, tran_status_cd, purchase_cost, purchase_qty
								,uniq_prods_qty, purchase_rev, tran_start_dttm, tran_end_dttm, purchase_date)
			values (r.sales_tran_id, r.individual_party_id, r.associate_party_id, r.location_id, r.tran_type_cd, r.tran_status_cd, r.mkb_cost_amt, r.mkb_item_qty
				,r.mkb_number_unique_items_qty, r.mkb_rev_amt, r.tran_start_dttm_dd, r.tran_end_dttm_dd, r.tran_date);
		END IF;
	END LOOP;

--	delete from schema20.stg_sales_transaction;
end $$;


-- Функция заполнения dds_receipt_details (один входной параметр: инкрементальная дата)
create or replace function schema20.dds_fill_dim_receipt_details(in inc_dt date)
returns VOID language sql volatile execute on any as $$


	with stg_sales_transaction_line_agg as(
		select sales_tran_id 
			,item_id 
			,avg(unit_selling_price_amt) unit_selling_price_amt
			,avg(unit_cost_amt) unit_cost_amt
			,sum(item_qty) item_qty
		from schema20.stg_sales_transaction_line stl
		where tran_line_date = inc_dt
			and not exists (
				select 1
				from schema20.dds_receipt_details rd
				where rd.receipt_id = stl.sales_tran_id
					and rd.product_id = stl.item_id
			)
		group by sales_tran_id, item_id
	)
	-- Заполнение таблицы-фактов инвентаризации на точках
	insert into schema20.dds_receipt_details
	select sales_tran_id 
		,item_id 
		,unit_selling_price_amt
		,unit_cost_amt
		,item_qty
	from stg_sales_transaction_line_agg stla;

	-- Обновление атрибутов dds_receipt_details 
	with stg_sales_transaction_line_agg as(
		select sales_tran_id 
			,item_id 
			,avg(unit_selling_price_amt) unit_selling_price_amt
			,avg(unit_cost_amt) unit_cost_amt
			,sum(item_qty) item_qty
		from schema20.stg_sales_transaction_line stl
		where tran_line_date = inc_dt
			and exists (
				select 1
				from schema20.dds_receipt_details rd
				where rd.receipt_id = stl.sales_tran_id
					and rd.product_id = stl.item_id
			)
		group by sales_tran_id, item_id
	)
	update schema20.dds_receipt_details rd
	set unit_selling_price = unit_selling_price_amt
		,unit_cost_amount = unit_cost_amt
		,quantity = item_qty
	from stg_sales_transaction_line_agg stla;

--	delete from schema20.stg_sales_transaction_line;

$$;

-- Функция заполнения dds_receipt_details (два входных параметра: начальная и конечная даты)
create or replace function schema20.dds_fill_dim_receipt_details(in start_dt date, in end_dt date)
returns VOID language sql volatile execute on any as $$


	with stg_sales_transaction_line_agg as(
		select sales_tran_id 
			,item_id 
			,avg(unit_selling_price_amt) unit_selling_price_amt
			,avg(unit_cost_amt) unit_cost_amt
			,sum(item_qty) item_qty
		from schema20.stg_sales_transaction_line stl
		where tran_line_date between start_dt and end_dt
			and not exists (
				select 1
				from schema20.dds_receipt_details rd
				where rd.receipt_id = stl.sales_tran_id
					and rd.product_id = stl.item_id
			)
		group by sales_tran_id, item_id
	)
	-- Заполнение таблицы-фактов инвентаризации на точках
	insert into schema20.dds_receipt_details
	select sales_tran_id 
		,item_id 
		,unit_selling_price_amt
		,unit_cost_amt
		,item_qty
	from stg_sales_transaction_line_agg stla;

	-- Обновление атрибутов dds_receipt_details 
	with stg_sales_transaction_line_agg as(
		select sales_tran_id 
			,item_id 
			,avg(unit_selling_price_amt) unit_selling_price_amt
			,avg(unit_cost_amt) unit_cost_amt
			,sum(item_qty) item_qty
		from schema20.stg_sales_transaction_line stl
		where tran_line_date between start_dt and end_dt
			and not exists (
				select 1
				from schema20.dds_receipt_details rd
				where rd.receipt_id = stl.sales_tran_id
					and rd.product_id = stl.item_id
			)
		group by sales_tran_id, item_id
	)
	update schema20.dds_receipt_details rd
	set unit_selling_price = unit_selling_price_amt
		,unit_cost_amount = unit_cost_amt
		,quantity = item_qty
	from stg_sales_transaction_line_agg stla;

--	delete from schema20.stg_sales_transaction_line;

$$;

