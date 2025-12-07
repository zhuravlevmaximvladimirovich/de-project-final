from airflow.decorators import dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pendulum
import datetime


PG_HOOK = PostgresHook(postgres_conn_id="postgres_connect")
VERTICA_HOOK = VerticaHook(vertica_conn_id="vertica_connect")

PG_SCHEMA = "DE_FINAL_PROJECT_STAGING"
VERTICA_SCHEMA = "VT251201B6F661__STAGING"
CURRENCIES_TABLE_NAME = "currencies_source"
TRANSACTIONS_TABLE_NAME = "transactions_source"

def import_data_from_postgresql_to_vertica_currencies_source(**kwargs):
    sql_query = f"""
    select
        date_update,
        currency_code,
        currency_code_with,
        currency_code_div
    from {PG_SCHEMA}.{CURRENCIES_TABLE_NAME}
    where date_update::date = '{kwargs["ds"]}'::date;
    """
    records = PG_HOOK.get_records(sql_query)
    for record in records:
        insert_sql = f"""INSERT INTO {VERTICA_SCHEMA}.{CURRENCIES_TABLE_NAME}
        (date_update, currency_code, currency_code_with, currency_code_div)
        VALUES ('{record[0]}'::timestamp, '{record[1]}'::int, '{record[2]}'::int, '{record[3]}'::numeric(5,2));"""
        VERTICA_HOOK.run(sql=insert_sql)

def import_data_from_postgresql_to_vertica_transactions_source(**kwargs):
    sql_query = f"""
    select
        operation_id,
        account_number_from,
        account_number_to,
        currency_code,
        country,
        status,
        transaction_type,
        amount,
        transaction_dt
    from {PG_SCHEMA}.{TRANSACTIONS_TABLE_NAME}
    where transaction_dt::date = '{kwargs["ds"]}'::date;
    """
    records = PG_HOOK.get_records(sql_query)
    for record in records:
        insert_sql = f"""INSERT INTO {VERTICA_SCHEMA}.{TRANSACTIONS_TABLE_NAME}
        (operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt)
        VALUES (
        '{record[0]}'::uuid, '{record[1]}'::int, '{record[2]}'::int, '{record[3]}'::numeric(3),
        '{record[4]}'::varchar(30), '{record[5]}'::varchar(30), '{record[6]}'::varchar(30), '{record[7]}'::int, '{record[8]}'::timestamp);"""
        VERTICA_HOOK.run(sql=insert_sql)


@dag(schedule_interval=None, start_date=pendulum.parse("2022-10-01"))
def import_data_from_postgresql_to_vertica():
    
    import_data_from_postgresql_to_vertica_currencies_source_task = PythonOperator(
        task_id="import_data_from_postgresql_to_vertica_currencies_source",
        python_callable=import_data_from_postgresql_to_vertica_currencies_source,
        provide_context=True
    )

    import_data_from_postgresql_to_vertica_transactions_source_task = PythonOperator(
        task_id="import_data_from_postgresql_to_vertica_transactions_source",
        python_callable=import_data_from_postgresql_to_vertica_transactions_source,
        provide_context=True
    )
    
    import_data_from_postgresql_to_vertica_currencies_source_task >> import_data_from_postgresql_to_vertica_transactions_source_task

_ = import_data_from_postgresql_to_vertica()