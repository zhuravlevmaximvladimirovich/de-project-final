from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import PythonOperator

import pendulum

PG_HOOK = PostgresHook(postgres_conn_id="postgres_connect")
VERTICA_HOOK = VerticaHook(vertica_conn_id="vertica_connect")

def import_data_from_postgresql_to_vertica_сurrencies_source():


    sql_query = f"""
    select
        date_update,
        currency_code,
        currency_code_with,
        currency_code_div
    from DE_FINAL_PROJECT_STAGING.сurrencies_source
    where date_update::date = ${ds};
    """
    records = PG_HOOK.get_records(sql_query)


    insert_sql = """INSERT INTO VT251201B6F661__STAGING.currencies_source
    (date_update, currency_code, currency_code_with, currency_code_div)
    VALUES (%s, %s, %s, %s);"""
    VERTICA_HOOK.run(sql=insert_sql, parameters=records)

with DAG(
    dag_id='import_data_from_postgresql_to_vertica',
    start_date=pendulum.parse('2022-10-01'),
    schedule_interval=None,
    catchup=False,
    tags=['import_data_from_postgresql_to_vertica'],
) as dag:
    migrate_task_сurrencies_source = PythonOperator(
        task_id='import_data_from_postgresql_to_vertica',
        python_callable=import_data_from_postgresql_to_vertica,
    )

def import_data_from_postgresql_to_vertica_transactions_source():

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
    from DE_FINAL_PROJECT_STAGING.transactions_source
    where transaction_dt::date = ${ds};
    """
    records = PG_HOOK.get_records(sql_query)


    insert_sql = """INSERT INTO VT251201B6F661__STAGING.transactions_source (
    operation_id, account_number_from, account_number_to, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    VERTICA_HOOK.run(sql=insert_sql, parameters=records)

with DAG(
    dag_id='import_data_from_postgresql_to_vertica_transactions_source',
    start_date=pendulum.parse('2022-10-01'),
    schedule_interval=None,
    catchup=False,
    tags=['import_data_from_postgresql_to_vertica_transactions_source'],
) as dag:
    migrate_task_transactions_source = PythonOperator(
        task_id='import_data_from_postgresql_to_vertica_transactions_source',
        python_callable=import_data_from_postgresql_to_vertica_transactions_source,
    )
    
    
    migrate_task_сurrencies_source >> migrate_task_transactions_source