	delete from schema20.dm_sales_margins;

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
