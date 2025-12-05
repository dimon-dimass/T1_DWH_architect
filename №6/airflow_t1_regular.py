from datetime import datetime
import pendulum

from airflow.sdk import DAG,task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_REGULAR_ID = "gp_etl_dag_regular"

# ETL конвейер с регулярной (ежедневной) загрузкой данных
with DAG(
    dag_id=DAG_REGULAR_ID,
    start_date=datetime.datetime(2023, 12, 1),
    schedule="0 0 * * *",
    tags=["greenplum","etl","regular"],
    default_args={"retries": 2, "conn_id": "gp_student20_conn"}
):
    @task_group()
    def stg_fill():

        stg_fill_ref_tbls_task = SQLExecuteQueryOperator(
            task_id = "stg_fill_ref_tbls_task",
            sql="sql/regular_queries/stg_fill_ref_tbls.sql",
        )

        stg_fill_dim_sales_trans_task = SQLExecuteQueryOperator(
            task_id = "stg_fill_dim_sales_trans_task",
            sql="sql/regular_queries/stg_fill_dim_sales_trans.sql",
            params={"start_date": {{ data_interval_start.to_date_string() }}}
        )

        stg_fill_dim_sales_tran_lines_task = SQLExecuteQueryOperator(
            task_id = "stg_fill_dim_sales_tran_lines_task",
            sql="sql/regular_queries/stg_fill_dim_sales_tran_lines.sql",
            params={"start_date": {{ data_interval_start.to_date_string() }}}
        )

        stg_fill_dim_item_inventory_task = SQLExecuteQueryOperator(
            task_id = "stg_fill_dim_item_inventory_task",
            sql="sql/regular_queries/stg_fill_dim_item_inventory.sql",
            params={"start_date": {{ data_interval_start.to_date_string() }}}
        )

        drop_col_algorithm_task = SQLExecuteQueryOperator(
            task_id="drop_col_algorithm_task",
            sql="sql/regular_queries/drop_col_algorithm.sql"
        )

        stg_fill_ref_tbls_task >> stg_fill_dim_sales_trans_task >> stg_fill_dim_sales_tran_lines_task >> stg_fill_dim_item_inventory_task >> drop_col_algorithm_task

    @task_group()
    def dds_fill():

        dds_fill_ref_categories_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_categories_task",
            sql="sql/regular_queries/dds_fill_ref_categories.sql",
        )

        dds_fill_ref_product_types_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_types_task",
            sql="sql/regular_queries/dds_fill_ref_product_types.sql",
        )

        dds_fill_ref_countries_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_countries_task",
            sql="sql/regular_queries/dds_fill_ref_countries.sql",
        )

        dds_fill_ref_divisions_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_divisions_task",
            sql="sql/regular_queries/dds_fill_ref_divisions.sql",
        )

        dds_fill_ref_regions_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_regions_task",
            sql="sql/regular_queries/dds_fill_ref_regions.sql",
        )

        dds_fill_ref_locations_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_locations_task",
            sql="sql/regular_queries/dds_fill_ref_locations.sql",
        )

        dds_fill_ref_product_brands_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_brands_task",
            sql="sql/regular_queries/dds_fill_ref_product_brands.sql",
        )

        dds_fill_ref_product_manufactures_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_manufactures_task",
            sql="sql/regular_queries/dds_fill_ref_product_manufactures.sql",
        )

        dds_fill_ref_product_baskets_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_baskets_task",
            sql="sql/regular_queries/dds_fill_ref_product_baskets.sql",
        )

        dds_fill_ref_product_commodities_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_commodities_task",
            sql="sql/regular_queries/dds_fill_ref_product_commodities.sql",
        )

        dds_fill_ref_products_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_products_task",
            sql="sql/regular_queries/dds_fill_ref_products.sql",
        )

        dds_fill_ref_customers_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_customers_task",
            sql="sql/regular_queries/dds_fill_ref_customers_task.sql",
        )

        dds_fill_dim_location_inventory_task = SQLExecuteQueryOperator(
            task_id="dds_fill_dim_location_inventory_task",
            sql="sql/regular_queries/dds_fill_dim_location_inventory.sql",\
            params={"start_date": {{ data_interval_start.to_date_string() }}}
        )

        dds_fill_ref_prod_inventory_price_history_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_prod_inventory_price_history_task",
            sql="sql/regular_queries/dds_fill_ref_prod_inventory_price_history.sql",
            params={"start_date": {{ data_interval_start.to_date_string() }}}
        )

        dds_fill_dim_receipts_task = SQLExecuteQueryOperator(
            task_id="dds_fill_dim_receipts_task",
            sql="sql/regular_queries/dds_fill_dim_receipts.sql",
        )

        dds_fill_dim_receipt_details_task = SQLExecuteQueryOperator(
            task_id="dds_fill_dim_receipt_details_task",
            sql="sql/regular_queries/dds_fill_dim_receipt_details.sql",
            params={"start_date": {{ data_interval_start.to_date_string() }}}
        )

        (dds_fill_ref_categories_task >> dds_fill_ref_product_types_task >> dds_fill_ref_countries_task >> dds_fill_ref_divisions_task >> dds_fill_ref_regions_task >>
         dds_fill_ref_locations_task >> dds_fill_ref_product_brands_task >> dds_fill_ref_product_manufactures_task >> dds_fill_ref_product_baskets_task >>
         dds_fill_ref_product_commodities_task >> dds_fill_ref_products_task >> dds_fill_ref_customers_task >> dds_fill_dim_location_inventory_task >> 
         dds_fill_ref_prod_inventory_price_history_task >> dds_fill_dim_receipts_task >> dds_fill_dim_receipt_details_task
        )
    
    @task_group()
    def dm_fill():

        dm_fill_sales_margin_task = SQLExecuteQueryOperator(
            task_id="dm_fill_sales_margin_task",
            sql="sql/regular_queries/dm_fill_sales_margin.sql",
            params={"start_date": {{ data_interval_start.to_date_string() }}}
        )

        dm_fill_sales_matrix_2022_2023_task = SQLExecuteQueryOperator(
            task_id="dm_fill_sales_matrix_2022_2023_task",
            sql="sql/regular_queries/dm_fill_sales_matrix_2022_2023.sql",
        )

        dm_fill_sales_margin_task >> dm_fill_sales_matrix_2022_2023_task

    stg_fill() >> dds_fill() >> dm_fill()