from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator

from airflow.operators.python import PythonOperator

import pendulum

SQL_FILE_PATH = "sql/vertica/dwh/"

@dag(schedule_interval=None, start_date=pendulum.parse("2022-10-01"))
def dag_import_data_from_stg_to_dwh_vertica():
    start_hubs_load = DummyOperator(task_id="start_hubs_load")
    end_hubs_load = DummyOperator(task_id="end_hubs_load")

    start_links_load = DummyOperator(task_id="start_links_load")
    end_links_load = DummyOperator(task_id="end_links_load")

    start_satellites_load = DummyOperator(task_id="start_satellites_load")
    end_satellites_load = DummyOperator(task_id="end_satellites_load")

    import_data_to_h_accounts = VerticaOperator(
        task_id="import_data_to_h_accounts",
        sql=f"{SQL_FILE_PATH}INSERT_h_accounts.sql",
        vertica_conn_id="vertica_connect"
    )
    import_data_to_h_countries = VerticaOperator(
        task_id="import_data_to_h_countries",
        sql=f"{SQL_FILE_PATH}INSERT_h_countries.sql",
        vertica_conn_id="vertica_connect"
    )
    import_data_to_h_currencies = VerticaOperator(
        task_id="import_data_to_h_currencies",
        sql=f"{SQL_FILE_PATH}INSERT_h_currencies.sql",
        vertica_conn_id="vertica_connect"
    )
    import_data_to_h_transactions = VerticaOperator(
        task_id="import_data_to_h_transactions",
        sql=f"{SQL_FILE_PATH}INSERT_h_transactions.sql",
        vertica_conn_id="vertica_connect"
    )
    import_data_to_l_account_transaction = VerticaOperator(
        task_id="import_data_to_l_account_transaction",
        sql=f"{SQL_FILE_PATH}INSERT_l_account_transaction.sql",
        vertica_conn_id="vertica_connect"
    )
    import_data_to_l_currency_pair = VerticaOperator(
        task_id="import_data_to_l_currency_pair",
        sql=f"{SQL_FILE_PATH}INSERT_l_currency_pair.sql",
        vertica_conn_id="vertica_connect"
    )
    import_data_to_l_transaction_country = VerticaOperator(
        task_id="import_data_to_l_transaction_country",
        sql=f"{SQL_FILE_PATH}INSERT_l_transaction_country.sql",
        vertica_conn_id="vertica_connect"
    )
    import_data_to_l_transaction_currency = VerticaOperator(
        task_id="import_data_to_l_transaction_currency",
        sql=f"{SQL_FILE_PATH}INSERT_l_transaction_currency.sql",
        vertica_conn_id="vertica_connect"
    )
    import_data_to_s_currency_pair = VerticaOperator(
        task_id="import_data_to_s_currency_pair",
        sql=f"{SQL_FILE_PATH}INSERT_s_currency_pair.sql",
        vertica_conn_id="vertica_connect"
    )
    import_data_to_s_transaction_status = VerticaOperator(
        task_id="import_data_to_s_transaction_status",
        sql=f"{SQL_FILE_PATH}INSERT_s_transaction_status.sql",
        vertica_conn_id="vertica_connect"
    )
    
    start_hubs_load >> [import_data_to_h_accounts, import_data_to_h_countries, import_data_to_h_currencies, import_data_to_h_transactions] >> end_hubs_load\
    >>\
     start_links_load >> [import_data_to_l_account_transaction, import_data_to_l_currency_pair, import_data_to_l_transaction_country, import_data_to_l_transaction_currency] >> end_links_load\
    >>\
     start_satellites_load >> [import_data_to_s_currency_pair, import_data_to_s_transaction_status] >> end_satellites_load

_ = dag_import_data_from_stg_to_dwh_vertica()