/*
	 СКРИПТЫ СОЗДАНИЯ ТАБЛИИЦ ДЕТАЛЬНОГО СЛОЯ
*/


-- Таблица-справочник стран
create table if not exists dds_countries(
	country_id smallint primary key,
	country_name varchar(56)
);

-- Таблица-справочник подразделений компании
create table if not exists dds_divisions(
	division_id serial primary key,
	src_division_id char(5),
	division_name varchar(100),
	country_id smallint references dds_countries,
	division_mgr_id integer references dds_employees,
	valid_from date,
	valid_until date
);

-- Таблица-справочник областей/регионов
create table if not exists dds_regions(
	region_id serial primary key,
	src_region_id char(5),
	region_name varchar(100),
	division_id smallint references dds_divisions,
	region_mgr_id integer references dds_employees,
	valid_from date,
	valid_until date
);

-- Таблица-справочник населенных пунктов
--create table if not exists dds_towns(
--	town_id serial primary key,
--	src_town_id integer,
--	town_name varchar(75),
--	region_id integer references dds_regions,
--	valid_from date,
--	valid_until date
--);
--
-- Таблица-справочник улиц
--create table if not exists dds_streets(
--	street_id serial primary key,
--	src_street_id integer,
--	street_name varchar(125),
--	town_id integer references dds_towns,
--	valid_from date,
--	valid_until date
--);
--
-- Таблица-справочник строений, домов
--create table if not exists dds_buildings(
--	building_id serial primary key,
--	src_building_id integer,
--	building_number varchar(25),
--	street_id smallint references dds_streets,
--	valid_from date,
--	valid_until date
--);
--
-- Таблица-справочник категорий товаров
create table if not exists dds_categories(
	category_id smallint primary key,
	category_name varchar(50),
	description varchar(100)
);


-- Таблица-справочник типов товаров
create table if not exists dds_product_types(
	product_type_id smallint primary key,
	product_type_name varchar(30),
	category_id smallint references dds_categories,
	description varchar(100)
);

-- Таблица-справочник поставщиков
create table if not exists dds_suppliers(
	supplier_id serial primary key,
	src_supplier_id varchar(100), -- не уверен в каком формате хранят такие атрибуты
	inn varchar(12), -- на случай, если сделки бывают с частниками, определю (12)
	bic varchar(10),
	description varchar(100),
	valid_from timestamp, -- поле для сохранения историчности
	valid_until timestamp -- поле для сохранения историчности
);

-- Таблица-справочник компаний-доставщиков
create table if not exists dds_delivery_companies(
	delivery_company_id serial primary key,
	src_delivery_company_id varchar(100),
	delivery_company_name varchar(100),
	inn varchar(12), -- на случай, если сделки бывают с частниками, определю (12)
	bic varchar(10),
	description varchar(100),
	valid_from timestamp, -- поле для сохранения историчности
	valid_until timestamp -- поле для сохранения историчности
);

-- Таблица-справочник машин-доставщиков
create table if not exists dds_vehicles(
	vehicle_id integer primary key,
	license_plate varchar(10),
	vehicle_type varchar(25),
	vahicle_name varchar(50),
	vehicle_capacity_unit varchar(25),
	vehicle_capacity numeric(9,3),
	delivery_company_id integer references dds_delivery_companies
);

-- Таблица-событие договоров с компаниями-доставщиками
create table if not exists dds_delivery_contracts(
	delivery_contract_id integer primary key,
	delivery_company_id integer references dds_delivery_companies,
	company_id integer, 
	start_date date,
	exp_date date,
	schedule varchar(100),
	currency varchar(25),
	price numeric(15,2),
	other_terms varchar(500)
);

-- Таблица-событие договоров с поставщиками
create table if not exists dds_supply_contracts(
	supply_contract_id integer primary key,
	supplier_id integer references dds_suppliers,
	delivery_contract_id integer references dds_delivery_contracts,
	start_date date, 
	exp_date date,
	schedule varchar(100),
	currency varchar(25),
	unit_price numeric(15,2),
	amount integer,
	other_terms varchar(500)	
);

-- Таблица-справочник брендов товаров
create table if not exists dds_product_brands(
	brand_id integer primary key,
	brand_name varchar(50),
	description varchar(200) -- ответственный за работу с брендом
);

-- Таблица-справочник изготовителей/производителей товаров
create table if not exists dds_product_manufactures(
	manufacture_id integer primary key,
	manufacture_name varchar(120),
	description varchar(200)
);

-- Таблица-справочник изготовителей/производителей товаров
create table if not exists dds_product_baskets(
	basket_id smallint primary key,
	basket_name varchar(120),
	description varchar(200)
);

-- Таблица-справочник изготовителей/производителей товаров
create table if not exists dds_product_commodities(
	commodity_id smallint primary key,
	commodity_name varchar(120),
	basket_id integer references dds_product_baskets,
	description varchar(200)
);

-- Таблица-справочник товаров
create table if not exists dds_products(
	product_id numeric(15) primary key,
	product_name varchar(100),
	product_type_id smallint references dds_product_types,
	product_category_id smallint references dds_categories(category_id),
	supply_contract_id integer references dds_supply_contracts,
	brand_id integer references dds_product_brands,
	manufacturer_id integer references dds_product_manufactures(manufacture_id),
	commodity_id smallint references dds_product_commodities
);

-- Таблица-справочник банков, предоставляющих экваиринг
create table if not exists dds_acquiring_banks(
	acq_bank_id integer primary key,
	src_acq_bank_id varchar(100),
	acq_bank_name varchar(100),
	inn varchar(10),
	bic varchar(10),
	description varchar(100),
	valid_from timestamp,
	valid_until timestamp
);

-- Таблица-справочник должностей, профессий компании
create table if not exists dds_jobs(
	job_id smallint primary key,
	job_name varchar(30),
	salary_amount numeric(10,2),
	description varchar(200)
);

-- Таблица-справочник работников компании
create table if not exists dds_employees(
	employee_id integer primary key,
	location_id integer,
	job_id smallint references dds_jobs,
	first_name varchar(20),
	last_name varchar(50),
	patronymic varchar(30),
	birth_date date,
	hire_date date,
	reports_to integer references dds_employees
);

-- Таблица-справочник отделов/магазинов
create table if not exists dds_locations(
	location_id serial primary key,
	src_location_id integer,
	location_name varchar(200),
	region_id integer references dds_regions,
	location_type varchar(100),
	location_budget numeric(15,2),
	manager_employee_id integer references dds_employees (employee_id),
	valid_from date, -- фиксируется "историчность" управляющего отдела/магазина
	valid_until date
);

-- Таблица-справочник причин изменения цен 
create table if not exists dds_price_change_reasons(
	change_reason_cd smallint primary key,
	change_desc varchar(30)
);

-- Таблица-справочник изменения цен товаров в точках
create table if not exists dds_prod_inventory_price_history(
	price_history_id serial primary key,
	product_id numeric(15) references dds_products,
	location_id integer references dds_locations,
	unit_type varchar(50), -- думаю лучше выставить больше, т.к. значение может быть задано полное (штука, киллограмм и т.д.)
	quantity_per_unit smallint,
	unit_price numeric(7,2),
	change_reason_cd char(3) references dds_price_change_reasons,
	valid_from date, -- фиксируется "историчность" управляющего отдела/магазина
	valid_until date
);

-- Таблица-событие соглашений, предоставляющих экваиринг
create table if not exists dds_acquiring_agreements(
	acq_agreement_id integer primary key,
	location_id integer references dds_locations,
	acq_bank_id integer references dds_acquiring_banks,
	start_date date,
	exp_date date,
	commission_rate numeric(4,3) default 0.0,
	terms varchar(500)
);

alter table dds_employees add foreign key (location_id) references dds_locations;

-- Таблица-справочник касс
create table if not exists dds_checkouts(
	checkout_id integer primary key,
	checkout_number integer,
	location_id integer references dds_locations,
	checkout_type varchar(25)
);

-- Таблица-справочник смен
create table if not exists dds_shifts(
	shift_id integer primary key,
	employee_id integer references dds_employees,
	job_id integer references dds_jobs,
	additional_info varchar(150),
	start_time timestamp,
	end_time timestamp
);

-- Таблица-события приемки доставок
create table if not exists dds_delivery_appointments(
	location_id integer references dds_locations,
	product_id numeric(15) references dds_products,
	appointment_datetime timestamp,
	amount integer,
	vehicle_id integer references dds_vehicles,
	delivery_contract_id integer references dds_delivery_contracts,
	responsible_for_id integer references dds_employees,
	primary key (location_id, product_id, appointment_datetime)
);

-- Таблица-события учета товара
create table if not exists dds_location_inventory(
	location_id integer references dds_locations,
	product_id numeric(15) references dds_products,
	inventory_datetime timestamp,
	stock_qty integer,
	stock_retail_price numeric(18,4),
	stock_cost_price numeric(18,4),
	on_order_qty integer,
	lost_sales_day_ind char(3),
	primary key (location_id, product_id, inventory_datetime)
);

-- Таблица-справочник покупателей/клиентов
create table if not exists dds_customers(
	customer_id serial primary key,
	src_customer_id integer,
	gender_type_cd char(1),
	customer_card_cred varchar(100),
	credentials varchar(300),
	city varchar(30),
	valid_from date,
	valid_until date
);

-- Таблица-справочник чеков
create table if not exists dds_receipts(
	receipt_id integer primary key,
	customer_id integer references dds_customers, -- id покупателя
	employee_id integer references dds_employees, -- id продавца-кассира
	location_id integer references dds_locations, -- id точки/отдела
	checkout_id integer references dds_checkouts,
	tran_type_cd char(1), -- тип транзакции 
	tran_status_cd char(1), -- статус транзакции
	purchase_cost numeric(18,4), -- сумма оплаты
	purchase_qty integer, -- количество товаров в чеке
	uniq_prods_qty integer, -- количество уникальных товаров в чеке
	purchase_rev numeric(18,4), -- сумма выручки
	tran_start_dttm timestamp(6), -- время начала транзакции
	tran_end_dttm timestamp(6), -- время завешения транзакции
	purchase_date date -- дата 
);

-- Таблица-события детализации чеков
create table if not exists dds_receipt_details(
	receipt_id integer references dds_receipts,
	product_id numeric(15) references dds_products,
	unit_selling_price numeric(8,4), -- цена товара за у.е. (по чеку)
	unit_cost_amount numeric(8,4), -- стоимость товара за у.е.
	quantity numeric(9,3), -- количество товара в чеке
	primary key(receipt_id, product_id)
);

-- Таблица-справочник транзакций, относящихся к банковским картам
create table if not exists dds_card_transactions(
	card_transaction_id integer primary key,
	acq_agreement_id integer references dds_acquiring_agreements,
	receipt_id integer references dds_receipts,
	bank_transaction_id varchar(100),
	transaction_datetime timestamp,
	status varchar(25)
)



/*
	СКРИПТЫ СОЗДАНИЯ ВИТРИН
*/

-- Витрина маржинальности продаж
create table if not exists dm_sales_margins(
	product_name varchar(50), -- полное имя товара
	product_category_name varchar(50), -- категория товара
	product_type varchar(30), -- тип товара
	brand_name varchar(50), -- имя бренда товара
	manufacture_name varchar(120), -- полное имя производителя товара
	department_name varchar(200), -- полное наименование отдела/точки магазина
	region_name varchar(100), -- область/регион (аргумент страны не стал указывать, думаю в этом нет необходимости)
	town_name varchar(75), -- населенный пункт
	street_n_building varchar(140), -- улица со строением
	checkout_number varchar(25), -- номер кассы
	effective_date date, -- дата фиксации продаж
	units_sold numeric(15,3), -- Метрика: количество продано
	sales_amount numeric(16,4), -- Метрика: сумма продаж
	sales_margin numeric(8,2) -- Метрика: маржинальность продаж в процентах 
);


-- Матричная витрина продаж по точкам с Января 2022 по Декабрь 2023
create table if not exists dm_sales_matrix_2022_2023(
	location_name varchar(200), -- наименование точки продаж
	jan_2022 numeric(14,2), -- продажи точек за Январь 2022
	feb_2022 numeric(14,2), -- продажи точек за Февраль 2022
	mar_2022 numeric(14,2), -- продажи точек за Март 2022
	apr_2022 numeric(14,2), -- продажи точек за Апрель 2022
	may_2022 numeric(14,2), -- продажи точек за Май 2022
	jun_2022 numeric(14,2), -- продажи точек за Июнь 2022
	jul_2022 numeric(14,2), -- продажи точек за Июль 2022
	aug_2022 numeric(14,2), -- продажи точек за Август 2022
	sep_2022 numeric(14,2), -- продажи точек за Сентябрь 2022
	oct_2022 numeric(14,2), -- продажи точек за Октябрь 2022
	nov_2022 numeric(14,2), -- продажи точек за Ноябрь 2022
	dec_2022 numeric(14,2), -- продажи точек за Декабрь 2022
	jan_2023 numeric(14,2), -- продажи точек за Январь 2023
	feb_2023 numeric(14,2), -- продажи точек за Февраль 2023
	mar_2023 numeric(14,2), -- продажи точек за Март 2023
	apr_2023 numeric(14,2), -- продажи точек за Апрель 2023
	may_2023 numeric(14,2), -- продажи точек за Май 2023
	jun_2023 numeric(14,2), -- продажи точек за Июнь 2023
	jul_2023 numeric(14,2), -- продажи точек за Июль 2023
	aug_2023 numeric(14,2), -- продажи точек за Август 2023
	sep_2023 numeric(14,2), -- продажи точек за Сентябрь 2023
	oct_2023 numeric(14,2), -- продажи точек за Октябрь 2023
	nov_2023 numeric(14,2), -- продажи точек за Ноябрь 2023
	dec_2023 numeric(14,2) -- продажи точек за Декабрь 2023
);
