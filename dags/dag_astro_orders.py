from datetime import datetime
from airflow.models import DAG

import pandas as pd
from pandas import DataFrame
from astro import sql as aql

from astro.files import File
from astro.sql.table import Table
# from logging import getLogger

# LOGGER =getLogger(__name__)

S3_FILE_PATH = "s3://nj-astrosdk-bucket"
S3_CONN_ID = "aws-astro-conn"
SNOWFLAKE_CONN_ID = "astro-snowflake-conn"
SNOWFLAKE_ORDERS = "orders_table"
SNOWFLAKE_FILTERED_ORDERS = "filtered_table"
SNOWFLAKE_JOINED = "joined_table"
SNOWFLAKE_CUSTOMERS = "customers_table"
SNOWFLAKE_REPORTING = "reporting_table"

## Using Astro-sdkaql.transform decorator to create different tasks
# Transform the data fetched from S3 and snowflake
@aql.transform
def filter_orders(input_table: Table):
    return "SELECT * FROM {{input_table}} WHERE amount > 150"

@aql.transform
def join_orders_customers(filtered_orders_table: Table, customers_table: Table):
    return """ SELECT c.customer_id, c.customer_name, order_id, purchase_date, amount, type
    FROM {{filtered_orders_table}} f JOIN {{customers_table}} c
    ON f.customer_id = c.customer_id
    """

## Transform SQL table to python dataframe using decorator aql.dataframe
## Be careful that the Sql or snowflake table will be loaded intO AIRFLOW environment - if data is huge, it can cause memory issues
@aql.dataframe(columns_names_capitalization="original")
def transform_dataframe(df: DataFrame):
    # LOGGER.info(f"type of reporting_table: {type(df)}")
    purchase_dates =  DataFrame(df["purchase_date"])
    print(f"purchase_dates: {purchase_dates}")
    print(f"purchase_dates type: {type(purchase_dates)}")
    return purchase_dates


with DAG(dag_id = "dag_orders_pipeline", start_date = datetime(2025,2,6),schedule_interval = "@daily", catchup=False):
    ## Task 1: Locad orders data from S3
    orders_data = aql.load_file(
        input_file = File(
            path = S3_FILE_PATH + "/orders_data_header.csv",
            conn_id = S3_CONN_ID
        ),
        output_table = Table(conn_id = SNOWFLAKE_CONN_ID)
    )

    customers_table = Table(name=SNOWFLAKE_CUSTOMERS, conn_id = SNOWFLAKE_CONN_ID)

    ## Define dependencies
    ## Task 2: filter_orders
    ## Task 3: join_orders_customers
    joined_data = join_orders_customers(filter_orders(orders_data), customers_table)

    ## Task 4: Merqe the joined data with the reporting table
    reporting_table = aql.merge(
        target_table = Table(name=SNOWFLAKE_REPORTING, conn_id = SNOWFLAKE_CONN_ID),
        source_table = joined_data,
        target_conflict_columns = ["order_id"],
        columns = ["customer_id", "customer_name"],
        if_conflicts = "update"
    )
    

    ## Task 5: Transform the data to python dataframe
    purchase_dates = transform_dataframe(reporting_table)

    ## Task 6: Cleanup the temporary tables that are not being used eventually
    purchase_dates >> aql.cleanup()
