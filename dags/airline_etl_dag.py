from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import logging

import config
import snowflake_sql

default_args = {
    'owner': 'airflow',
    'retries': 0,
}


def run_snowflake_query(query, fetch_result=True):
    hook = SnowflakeHook(snowflake_conn_id=config.CONN_ID)
    if fetch_result:
        result = hook.get_first(query)
        logging.info(f"Query Result: {result}")
        return result
    else:
        hook.run(query)


@dag(
    dag_id='airline_etl_production',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['snowflake', 'airline', 'production']
)
def airline_etl_flow():
    @task
    def upload_file_to_stage():
        run_snowflake_query(snowflake_sql.UPLOAD_TO_STAGE_SQL, fetch_result=False)
        logging.info("File upload command executed.")

    @task
    def load_raw_layer():
        run_snowflake_query(snowflake_sql.COPY_INTO_TABLE_SQL)

    @task
    def process_core_layer():
        run_snowflake_query(snowflake_sql.CALL_CORE_PROC_SQL)

    @task
    def update_data_marts():
        run_snowflake_query(snowflake_sql.CALL_MARTS_PROC_SQL)

    upload_file_to_stage() >> load_raw_layer() >> process_core_layer() >> update_data_marts()


etl_dag = airline_etl_flow()