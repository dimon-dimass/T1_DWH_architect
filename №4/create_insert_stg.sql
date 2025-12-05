
-- СКРИПТ ПРОВЕРКИ НА ПУСТОТУ СТОЛБЦОВ И ПОСЛЕДУЮЩЕЕ УДАЛЕНИЕ ИЗ ТАБЛИЦ ПОДГОТОВИТЕЛЬНОГО СЛОЯ

DO $$
DECLARE
	tbl RECORD; -- переменная, содержащая имена таблиц
	col RECORD; -- переменная, содержащая имена столбцов и их типов
	schema_name TEXT = 'schema20'; -- имя схемы
	non_empty_cols TEXT[] = '{}'; -- переменная, содержащая имена "пустых" столбцов
	create_stmt TEXT; -- переменная содержащая запрос на удаление "пустых" столбцов
	insert_stmt TEXT;
	flag boolean; -- флаг для блока проверки EXIST непустых значений столбца
BEGIN

	-- Определяем таблицы по которым будет проходить алгоритм
	FOR tbl IN (
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = schema_name AND table_name LIKE 'src%'
	) LOOP
		RAISE NOTICE 'Таблица %', tbl.table_name;
		non_empty_cols = '{}';

		-- Определяем столбцы каждой таблицы на проверку непустоты
		FOR col IN (
			SELECT column_name, data_type
			FROM information_schema.columns
			WHERE table_schema=schema_name AND table_name = tbl.table_name
		) LOOP
			-- для character столбцов отдельный блок с проверкой ''(длины 0) значений
			IF col.data_type LIKE '%char%' THEN
				EXECUTE format('SELECT EXISTS (
					SELECT 1
					FROM %I.%I
					WHERE %I IS NOT NULL 
						and length(%I) > 0
					LIMIT 1
				)',schema_name, tbl.table_name, col.column_name, col.column_name) INTO flag;
				IF flag 
				THEN
					RAISE NOTICE 'Столбец % непустой', col.column_name;
					non_empty_cols = non_empty_cols || format('%I %s', col.column_name, col.data_type);
				END IF;
			ELSE 
				-- блок для числовых столбцов
				EXECUTE format('SELECT EXISTS (
					SELECT 1
					FROM %I.%I
					WHERE %I IS NOT NULL
					LIMIT 1
				)',schema_name, tbl.table_name, col.column_name, col.column_name) INTO flag;
				IF flag 
				THEN
					RAISE NOTICE 'Столбец % непустой', col.column_name;
					non_empty_cols = non_empty_cols || format('%I %s', col.column_name, col.data_type);
				END IF;
			END IF;
		END LOOP;

		-- Формируем и применяем запрос на удаление столбцов 
		IF array_length(non_empty_cols, 1) > 0 THEN
			create_stmt = format(
				'CREATE TABLE %I.%I (%s)',
				schema_name,
				replace(tbl.table_name,'src','stg'),
				array_to_string(non_empty_cols, ', ')
			);
			EXECUTE create_stmt;

--			non_empty_cols = '{}';
--	
--			-- Определяем столбцы каждой таблицы на проверку непустоты
--			FOR col IN (
--				SELECT column_name
--				FROM information_schema.columns
--				WHERE table_schema=schema_name AND table_name = replace(tbl.table_name,'src','stg')
--			) LOOP
--				non_empty_column  = non_empty_column || format('%I', col.column_name)
--			END LOOP;
--	
--			insert_stmt = format('INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
--				schema_name, replace(tbl.table_name, 'src','stg'),
--				array_to_string(non_empty_columns, ', '),
--				array_to_string(non_empty_columns, ', '),
--				schema_name, tbl.table_name);
--			EXECUTE insert_stmt;

		END IF;

	END LOOP;
END $$;