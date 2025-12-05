/*
СКРИПТЫ СОЗДАНИЯ ВИТРИН
*/

create or replace function schema20.dm_fill_sales_margin(in inc_dt date)
returns VOID language sql volatile execute on any as $$

	-- Предварительная очистка витрины
	delete from schema20.dm_sales_margins
	where effective_date = inc_date;

	-- Скрипт наполнения витрины на отчетную дату inc_dt
    with prepared_for_dm as (
        select product_name -- p
            ,location_name -- d
            ,region_name -- rg
            ,division_name -- dv
            ,country_name -- c
            ,location_type -- department
            ,commodity_name -- p
            ,basket_name -- p
            ,category_name -- p
            ,product_type_name -- p
            ,brand_name --p
            ,manufacture_name -- p
            ,gender_type_cd -- c
            ,case when purchase_date < date_trunc('day',tran_end_dttm) 
                then date_trunc('day',tran_end_dttm)
                else purchase_date end purchase_date
            ,sum(quantity) as units_sold
            ,unit_selling_price * quantity as sales_amount
            ,round(((unit_selling_price - unit_cost_amount)/unit_selling_price)*100,2) as sales_margin
        from schema20.dds_receipt_details rd
        join schema20.dds_receipts r 
            on r.receipt_id = rd.receipt_id and r.tran_status_cd = 'S'
        join schema20.dds_locations l
            on r.location_id = l.location_id
        join schema20.dds_products p
            on p.product_id = rd.product_id
        join schema20.dds_product_commodities pc
            on pc.commodity_id = p.commodity_id
        join schema20.dds_product_basket pb
            on pb.basket_id = pc.basket_id
        join schema20.dds_product_manufactures pm
            on pm.manufacture_id = p.manufacture_id
        join schema20.dds_product_brands pbr
            on prb.brnad_id = p.product_brand_id
        join schema20.dds_product_types pt
            on pt.product_type_id = p.product_type_id
        join schema20.dds_categories ct
            on ct.category_id = p.product_category_id
        join schema20.dds_regions rg 
            on rg.region_id = d.region_id
        join schema20.dds_divisions dv
            on dv.division_id = rg.division_id
        join schema20.dds_countries c
            on c.country_id = dv.country_id
        join schema20.dds_customers cs
            on cs.customer_id = r.customer_id
		where purchase_date = inc_dt 
			or datetrunc('day',tran_end_dttm) = inc_dt
    )
	insert into schema20.dm_sales_margins
    select product_name
        ,location_name 
        ,region_name 
        ,division_name 
        ,country_name 
        ,location_type
        ,commodity_name 
        ,basket_name 
        ,category_name 
        ,product_type_name 
        ,brand_name 
        ,manufacture_name 
        ,gender_type_cd
        ,purchase_date
        ,sum(units_sold) units_sold
        ,sum(sales_amount) sales_amount
        ,avg(sales_margin) sales_margin
    from prepared_for_dm
    group by product_name 
        ,location_name 
        ,region_name 
        ,division_name 
        ,country_name 
        ,location_type 
        ,commodity_name 
        ,basket_name 
        ,category_name 
        ,product_type_name 
        ,brand_name 
        ,manufacture_name 
        ,gender_type_cd 
        ,purchase_date;

$$;


create or replace function schema20.dm_fill_sales_matrix_2022_2023()
returns VOID language plpgsql volatile execute on any as $$
declare
	r date;
	insert_stmt TEXT;
begin
	-- Предварительная очистка витрины
	truncate schema20.dm_sales_matrix_2022_2023;
	
	-- Алгоритм заполнения витрины
	insert_stmt = 'insert into schema20.dm_sales_matrix_2022_2023 select location_name';
	
	FOR r IN 
		SELECT generate_series('2022-01-01'::date,'2023-12-01'::date,'1 month'::interval)
	LOOP
		insert_stmt = insert_stmt || format(', coalesce(sum(purchase_cost) FILTER (where (purchased_date >= %L and purchased_date < (%L::date+interval''1 month'')) or (tran_end_dttm >= %L and tran_end_dttm < (%L::date+interval''1 month''))),0) as %I'
				,r
				,to_char(r,'Mon YYYY')
			); 
	END LOOP;
	
	insert_stmt = insert_stmt || ' from schema20.dds_receipts r
									join schema20.dds_locations l on l.location_id = r.location_id
									where (date_part(''year'',purchase_date) in (2022,2023)
											or date_part(''year'', tran_end_dttm) in (2022,2023))
											and tran_status_cd = ''S''
									group by l.location_name;';
	execute insert_stmt;
END $$;