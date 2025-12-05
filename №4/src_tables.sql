/*
	СКРИПТЫ СОЗДАНИЯ ВНЕШНИХ ТАБЛИЦ
*/

/*
СКРИПТЫ ВНЕШНИХ ТАБЛИЦ PXF:JDBC  
*/


-- клиенты компании, в том числе с картой лояльности 
create external table schema20.src_jdbc_customer(
	individual_party_id integer, --идентификатор клиента на источнике
	birth_dt date, --дата рождения клиента
	ethnicity_cd char(1), --код национальности (не заполняется)
	gender_type_cd char(1), --код пола клиента (M – мужской, F – женский)
	given_name varchar(100), --имя клиента
	middle_name varchar(100), --отчество клиента
	family_name varchar(100), --фамилия клиента
	name_prefix_txt varchar(50), --дополнительная приставка к ФИО (не заполняется)
	name_suffix_txt varchar(50), --дополнительный суффикс к ФИО (не заполняется)
	street_address varchar(100), --адрес по прописке (из анкеты клиента)
	city varchar(30), --город по прописке (из анкеты клиента)
	state_code varchar(2), --код штата по прописке (из анкеты клиента)
	zip_code char(5) --почтовый индекс (из анкеты клиента)
)
location ('pxf://mwdo_base.customer?PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://10.4.107.31:5432/adb&USER=student20&PASS=student20')
format 'CUSTOM'
(FORMATTER='pxfwritable_import');

-- номенклатура товаров, продающихся компанией. Строк: 8121. 
create external table schema20.src_jdbc_item(
	item_id numeric(15), --ИД товара
	item_name varchar(100), --название товара
	item_desc varchar(250), --описание товара
	item_subclass_cd integer, --категория товара
	item_type_cd smallint, --тип товара
	inventory_ind char(3), -- необходимость хранения в холодильнике
	vendor_party_id integer, -- ИД вендора (производителя) товара
	commodity_cd smallint, -- код индекса премиальности товара
	brand_cd integer --код бренда от производителя товара
)
location ('pxf://mwdo_base.item?PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://10.4.107.31:5432/adb&USER=student20&PASS=student20')
format 'CUSTOM'
(FORMATTER='pxfwritable_import');

-- остатки товаров на складах и полках точек продаж, снимающиеся 1 раз в неделю. 
create external table schema20.src_jdbc_item_inventory(
	location_id integer, --код точки продаж
	item_inv_dt date, --дата инвентаризации
	item_id numeric(15), --код товара
	on_hand_unit_qty integer, --количество товара в наличии
	on_hand_at_retail_amt numeric(18, 4), --розничная стоимость товара из наличия
	on_hand_cost_amt numeric(18, 4), --себестоимость товара из наличия
	on_order_qty integer, -- количество товара, которое требуется дозаказать для пополнения запаса.
	lost_sales_day_ind char(3), -- флаг просрочки срока годности
	item_inv_time timestamp(6) --дата-время инвентаризации 
)
location ('pxf://mwdo_base.item_inventory?PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://10.4.107.31:5432/adb&USER=student20&PASS=student20')
format 'CUSTOM'
(FORMATTER='pxfwritable_import');


-- история изменения стоимости товаров. Строк: 2,1-2,2 миллиона. 
create external table schema20.src_jdbc_item_price_history(
	location_id integer, --код точки продаж
	item_id numeric(15), --код товара
	item_price_start_dt date, --дата начала действия стоимости товара
	price_change_reason_cd char(3), --код причины изменения стоимости товара
	item_price_amt numeric(18,4), --стоимость товара
	current_indicator char(1) --флаг актуальной стоимости товара 
)
location ('pxf://mwdo_base.item_price_history?PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://10.4.107.31:5432/adb&USER=student20&PASS=student20')
format 'CUSTOM'
(FORMATTER='pxfwritable_import');

-- сводная информация по продажам товаров. Строк: около 11 миллионов. 
create external table schema20.src_jdbc_sales_transaction(
	sales_tran_id integer, --ИД факта продажи
	visit_id integer, --ИД визита покупателя на точку продаж
	location_id integer, --код точки продаж
	tran_status_cd char(1), --кода статуса проведения оплаты
	reported_as_dttm timestamp(6), --дата-время закрытия чека
	tran_type_cd char(1), --код типа оплаты
	mkb_cost_amt numeric(18,4), --сумма оплаты
	mkb_item_qty integer, --количество товаров в чеке
	mkb_number_unique_items_qty integer, --количество уникальных товаров в чеке
	mkb_rev_amt numeric(18,4), -- сумма выручки
	associate_party_id integer, -- ИД продавца-кассира
	individual_party_id integer, --ИД покупателя
	tran_start_dttm_dd timestamp(6), --дата-время начала проведения платежа
	tran_date date, --дата продажи
	tran_end_dttm_dd timestamp(6) --дата-время окончания проведения платежа
)
location ('pxf://mwdo_base.sales_transaction?PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://10.4.107.31:5432/adb&USER=student20&PASS=student20')
format 'CUSTOM'
(FORMATTER='pxfwritable_import');


-- детальная информация по продажам товаров (товары в чеках). Строк: 71-72 миллиона.
create external table schema20.src_jdbc_sales_transaction_line(
	sales_tran_id integer, --ИД факта продажи
	sales_tran_line_num smallint, --позиция товара в списке товаров в чеке
	item_id numeric(15), --код товара в чеке
	item_qty smallint, --количество товара в чеке
	unit_selling_price_amt numeric(8,4), --продажная стоимость товара
	unit_cost_amt numeric(8,4), --стоимость единицы товара
	tran_line_status_cd char(1), --код статуса проведения платежа
	sales_tran_line_start_dttm timestamp(6), --дата-время начала проведения первого товара через кассу
	tran_line_sales_type_cd char(2), --код конечного статуса покупки товара (зарезервировано для будущего использования)
	sales_tran_line_end_dttm timestamp(6), --дата-время окончания проведения последнего товара через кассу
	tran_line_date date, --дата продажи по чеку
	location_id integer--код точки продаж 
)
location ('pxf://mwdo_base.sales_transaction_line?PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://10.4.107.31:5432/adb&USER=student20&PASS=student20')
format 'CUSTOM'
(FORMATTER='pxfwritable_import');



/*
СКРИПТЫ ВНЕШНИХ ТАБЛИЦ GPFDIST:TEXT/CSV 
*/

-- Индексы премиальности товаров. Строк: 182.
create external table schema20.src_gpfdist_osh_commodity(
	commodity_cd smallint, --код индекса
	commodity_name varchar(40), -- название индекса
	grocery_basket_cd smallint -- код продуктовой корзины 
)
location(
	'gpfdist://10.4.107.31:8082/osh_commodity.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- Продуктовые корзины товаров. Строк: 9
create external table schema20.src_gpfdist_osh_grocery_basket(
	grocery_basket_cd smallint, -- код продуктовой корзины
	grocery_basket_desc varchar(100) -- описание продуктовой корзины
)
location(
	'gpfdist://10.4.107.31:8082/osh_grocery_basket.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- Категории продуктов. Строк: 444. 
create external table schema20.src_gpfdist_osh_item_subclass(
	item_subclass_cd smallint, -- код категории продукта
	item_subclass_name varchar(40) -- название категории продукта
)
location(
	'gpfdist://10.4.107.31:8082/osh_item_subclass.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- Тип продуктов. Строк: 1
create external table schema20.src_gpfdist_osh_item_type(
	item_type_cd smallint, -- код типа продукта
	item_type_name varchar(40) -- название типа продукта
)
location(
	'gpfdist://10.4.107.31:8082/osh_item_type.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- вся компания. Строк: 1.
create external table schema20.src_gpfdist_t_all_divisions(
	all_divisions_cd smallint, --код всей компании
	all_divisions_name varchar(10) -- название всей компании
)
location(
	'gpfdist://10.4.107.31:8082/t_all_divisions.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

--alter external table schema20.src_gpfdist_t_brand alter column brand_name type varchar(100);
-- бренды производителей товаров, продающихся компанией. Строк: 416. 
create external table schema20.src_gpfdist_t_brand(
	mfg integer, -- код производителя
	brand_cd smallint, --код бренда
	brand_name varchar(100), --название бренда (изменил на 100, т.к. при 30 выдавало ошибку!)
	brand_party_id smallint --ИД сотрудника, ответственного за бренд (бренд-менеджер) 
)
location(
	'gpfdist://10.4.107.31:8082/t_brand.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- каналы продаж. Строк: 1.
create external table schema20.src_gpfdist_t_channel(
	channel_cd smallint, --код канала продаж
	channel_desc varchar(15) --описание канала продаж  
)
location(
	'gpfdist://10.4.107.31:8082/t_channel.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- области РФ, в которой расположены торговые точки продаж компании. Строк: 52. 
create external table schema20.src_gpfdist_t_district(
	district_cd char(5), --код области РФ, в которой расположена торговая точка
	district_name varchar(30), --название области
	region_cd char(5), --код региона
	district_mgr_associate_id integer--ИД сотрудника, ответственного за выполнения плана продаж по региону. 
)
location(
	'gpfdist://10.4.107.31:8082/t_district.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- подразделения компании. Строк: 4.
create external table schema20.src_gpfdist_t_division(
	division_cd char(5), --код подразделения
	division_name varchar(10), --названия подразделения
	org_party_id integer, --внутренний ИД орг. структуры (не используется).
	all_divisions_cd smallint, --код всей компании
	division_mgr_associate_id integer --ИД сотрудника, ответственного за выполнения плана продаж по подразделению. 
)
location(
	'gpfdist://10.4.107.31:8082/t_division.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- точки продаж компании. Строк: 205.
create external table schema20.src_gpfdist_t_location(
	location_id smallint, --код точки продаж
	location_name varchar(40), --название магазина
	location_open_dt date, --дата открытия магазина
	location_close_dt date, --дата закрытия магазина
	location_effective_dt date, --дата закрытия магазина
	location_total_area_meas numeric(18,4), --общая площадь магазина
	chain_cd smallint, --код торговой сети (зарезервировано для будущего использования)
	channel_cd smallint, -- код канала продаж
	district_cd char(5), --код области
	parent_location_id smallint, -- код родительской точки продаж (зарезервировано для будущего использования)
	location_mgr_associate_id integer,--ИД главного менеджера точки продаж
	location_type_cd smallint --тип точки продаж 
)
location(
	'gpfdist://10.4.107.31:8082/t_location.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- тип точки продаж компании. Строк: 5.
create external table schema20.src_gpfdist_t_location_type(
	location_type_cd smallint, --код типа точки продаж
	location_type_desc varchar(30) --описание типа точки продаж
)
location(
	'gpfdist://10.4.107.31:8082/t_location_type.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- причины изменения цены товара на полках. Строк: 3.
create external table schema20.src_gpfdist_t_price_change_reason(
	price_change_reason_cd smallint, --код причины изменения цены.
	price_change_reason_desc varchar(30) --описание причины изменения цены.
)
location(
	'gpfdist://10.4.107.31:8082/t_price_change_reason.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- регионы РФ точек продаж компании. Строк: 20. 
create external table schema20.src_gpfdist_t_region(
	region_cd char(5), --код региона
	region_name varchar(40), --название региона
	division_cd char(5), --код подразделения, в который входит регион
	region_mgr_associate_id integer --ИД сотрудника, ответственного за выполнения плана продаж по региону.
)
location(
	'gpfdist://10.4.107.31:8082/t_region.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';

-- поставщики (производители) товара. Строк: 10.
create external table schema20.src_gpfdist_t_vendor(
	vendor_party_id integer, -- ИД поставщика товара
	vendor_name varchar(30) -- Название поставщика товара 
)
location(
	'gpfdist://10.4.107.31:8082/t_vendor.csv'
)
format 'CSV'(
	header
	delimiter ','
)
encoding 'UTF-8';
