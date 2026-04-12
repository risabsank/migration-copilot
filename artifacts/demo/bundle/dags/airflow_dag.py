from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG('migration_plan', start_date=datetime(2024,1,1), schedule=None, catchup=False) as dag:
    prepare = EmptyOperator(task_id='prepare')
    backfill = EmptyOperator(task_id='backfill')
    sync = EmptyOperator(task_id='sync')
    validate = EmptyOperator(task_id='validate')
    cutover = EmptyOperator(task_id='cutover')
    phased_cutover_domains = EmptyOperator(task_id='phased-cutover-domains')
