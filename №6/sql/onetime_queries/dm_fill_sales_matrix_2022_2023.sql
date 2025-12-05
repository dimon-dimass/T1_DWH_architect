do $$
declare
    r date;
    insert_stmt TEXT;
begin
    -- Предварительная очистка витрины
	truncate schema20.dm_sales_matrix_2022_2023;
	
	-- Алгоритм заполнения витрины
	insert_stmt = 'insert into schema20.dm_sales_matrix_2022_2023 select location_name';
	
	FOR r IN 
		SELECT generate_series('2022-01-01'::date,'2023-11-01'::date,'1 month'::interval)
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
