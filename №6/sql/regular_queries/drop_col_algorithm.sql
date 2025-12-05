DO $$
DECLARE
	tbl RECORD; -- переменная, содержащая имена таблиц
	col RECORD; -- переменная, содержащая имена столбцов и их типов
	schema_name TEXT = 'schema20'; -- имя схемы
	empty_cols TEXT[] = '{}'; -- переменная, содержащая имена "пустых" столбцов
	drop_stmt TEXT; -- переменная содержащая запрос на удаление "пустых" столбцов
	flag boolean; -- флаг для блока проверки EXIST непустых значений столбца
BEGIN

	-- Определяем таблицы по которым будет проходить алгоритм
	FOR tbl IN (
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = schema_name AND table_name LIKE 'stg%'
	) LOOP
		RAISE NOTICE 'Таблица %', tbl.table_name;
		empty_cols = '{}';

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
				IF NOT flag 
				THEN
					RAISE NOTICE 'Столбец % пустой', col.column_name;
					empty_cols = empty_cols || format('DROP COLUMN %I', col.column_name);
				END IF;

			ELSE 
				-- блок для числовых столбцов
				EXECUTE format('SELECT EXISTS (
					SELECT 1
					FROM %I.%I
					WHERE %I IS NOT NULL
					LIMIT 1
				)',schema_name, tbl.table_name, col.column_name, col.column_name) INTO flag;
				IF NOT flag 
				THEN
					RAISE NOTICE 'Столбец % пустой', col.column_name;
					empty_cols = empty_cols || format('DROP COLUMN %I', col.column_name);
				END IF;
			END IF;
		END LOOP;

		-- Формируем и применяем запрос на удаление столбцов 
		IF array_length(empty_cols, 1) > 0 THEN
			drop_stmt = format(
				'ALTER TABLE %I.%I %s',
				schema_name,
				tbl.table_name,
				array_to_string(empty_cols, ', ')
			);
			EXECUTE drop_stmt;
		END IF;
	END LOOP;
END $$;