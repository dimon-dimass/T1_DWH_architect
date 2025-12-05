from datetime import datetime

from airflow.sdk import DAG,task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.events import EventsTimetable

DAG_ONETIME_ID = "gp_etl_dag_onetime"

# ETL конвейер с разовой загрузкой данных
with DAG(
    dag_id=DAG_ONETIME_ID,
    start_date=datetime.datetime(2000, 1, 1),
    end_date=datetime.datetime(2023,11,30)
    schedule="@once",
    tags=["greenplum","etl","onetime"],
    default_args={"retries": 2, "conn_id": "gp_student20_conn"}
):
    @task_group()
    def stg_fill():

        stg_fill_ref_tbls_task = SQLExecuteQueryOperator(
            task_id = "stg_fill_ref_tbls_task",
            sql="sql/onetime_queries/stg_fill_ref_tbls.sql",
        )

        stg_fill_dim_sales_trans_task = SQLExecuteQueryOperator(
            task_id = "stg_fill_dim_sales_trans_task",
            sql="sql/onetime_queries/stg_fill_dim_sales_trans.sql",
        )

        stg_fill_dim_sales_tran_lines_task = SQLExecuteQueryOperator(
            task_id = "stg_fill_dim_sales_tran_lines_task",
            sql="sql/onetime_queries/stg_fill_dim_sales_tran_lines.sql",
        )

        stg_fill_dim_item_inventory_task = SQLExecuteQueryOperator(
            task_id = "stg_fill_dim_item_inventory_task",
            sql="sql/onetime_queries/stg_fill_dim_item_inventory.sql",
        )

        drop_col_algorithm_task = SQLExecuteQueryOperator(
            task_id="drop_col_algorithm_task",
            sql="sql/onetime_queries/drop_col_algorithm.sql"
        )

        stg_fill_ref_tbls_task >> stg_fill_dim_sales_trans_task >> stg_fill_dim_sales_tran_lines_task >> stg_fill_dim_item_inventory_task >> drop_col_algorithm_task

    @task_group()
    def dds_fill():

        dds_fill_ref_categories_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_categories_task",
            sql="sql/onetime_queries/dds_fill_ref_categories.sql",
        )

        dds_fill_ref_product_types_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_types_task",
            sql="sql/onetime_queries/dds_fill_ref_product_types.sql",
        )

        dds_fill_ref_countries_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_countries_task",
            sql="sql/onetime_queries/dds_fill_ref_countries.sql",
        )

        dds_fill_ref_divisions_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_divisions_task",
            sql="sql/onetime_queries/dds_fill_ref_divisions.sql",
        )

        dds_fill_ref_regions_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_regions_task",
            sql="sql/onetime_queries/dds_fill_ref_regions.sql",
        )

        dds_fill_ref_locations_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_locations_task",
            sql="sql/onetime_queries/dds_fill_ref_locations.sql",
        )

        dds_fill_ref_product_brands_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_brands_task",
            sql="sql/onetime_queries/dds_fill_ref_product_brands.sql",
        )

        dds_fill_ref_product_manufactures_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_manufactures_task",
            sql="sql/onetime_queries/dds_fill_ref_product_manufactures.sql",
        )

        dds_fill_ref_product_baskets_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_baskets_task",
            sql="sql/onetime_queries/dds_fill_ref_product_baskets.sql",
        )

        dds_fill_ref_product_commodities_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_product_commodities_task",
            sql="sql/onetime_queries/dds_fill_ref_product_commodities.sql",
        )

        dds_fill_ref_products_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_products_task",
            sql="sql/onetime_queries/dds_fill_ref_products.sql",
        )

        dds_fill_ref_customers_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_customers_task",
            sql="sql/onetime_queries/dds_fill_ref_customers_task.sql",
        )

        dds_fill_dim_location_inventory_task = SQLExecuteQueryOperator(
            task_id="dds_fill_dim_location_inventory_task",
            sql="sql/onetime_queries/dds_fill_dim_location_inventory.sql",
        )

        dds_fill_ref_prod_inventory_price_history_task = SQLExecuteQueryOperator(
            task_id="dds_fill_ref_prod_inventory_price_history_task",
            sql="sql/onetime_queries/dds_fill_ref_prod_inventory_price_history.sql",
        )

        dds_fill_dim_receipts_task = SQLExecuteQueryOperator(
            task_id="dds_fill_dim_receipts_task",
            sql="sql/onetime_queries/dds_fill_dim_receipts.sql",
        )

        dds_fill_dim_receipt_details_task = SQLExecuteQueryOperator(
            task_id="dds_fill_dim_receipt_details_task",
            sql="sql/onetime_queries/dds_fill_dim_receipt_details.sql",
        )

        (dds_fill_ref_categories_task >> dds_fill_ref_product_types_task >> dds_fill_ref_countries_task >> dds_fill_ref_divisions_task >> dds_fill_ref_regions_task >>
         dds_fill_ref_locations_task >> dds_fill_ref_product_brands_task >> dds_fill_ref_product_manufactures_task >> dds_fill_ref_product_baskets_task >>
         dds_fill_ref_product_commodities_task >> dds_fill_ref_products_task >> dds_fill_ref_customers_task >> dds_fill_dim_location_inventory_task >> 
         dds_fill_ref_prod_inventory_price_history_task >> dds_fill_dim_receipts_task >> dds_fill_dim_receipt_details_task
        )
    
    @task_group()
    def dm_fill():

        dm_fill_sales_margin = SQLExecuteQueryOperator(
            task_id="dm_fill_sales_margin",
            sql="sql/onetime_queries/dm_fill_sales_margin.sql",
        )

        dm_fill_sales_matrix_2022_2023 = SQLExecuteQueryOperator(
            task_id="dm_fill_sales_matrix_2022_2023",
            sql="sql/onetime_queries/dm_fill_sales_matrix_2022_2023.sql",
        )

        dm_fill_sales_margin >> dm_fill_sales_matrix_2022_2023

    stg_fill() >> dds_fill() >> dm_fill()