from airflow.decorators import dag
from airflow.providers.vertica.operators.vertica import VerticaOperator

import pendulum

SQL_FILE_PATH = "sql/vertica/dwh/"

@dag(schedule_interval=None, start_date=pendulum.parse('2022-10-01'))
def dag_import_data_to_global_metrics():

    import_data_to_global_metrics = VerticaOperator(
        task_id="import_data_to_global_metrics",
        sql=f"{SQL_FILE_PATH}INSERT_global_metrics.sql",
        vertica_conn_id="vertica_connect"
    )

    import_data_to_global_metrics

_ = dag_import_data_to_global_metrics()