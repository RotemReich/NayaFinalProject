from airflow.decorators import dag, task
import dag_assist_functions as daf
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


@dag(
    dag_id = "proccess_stores",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    # schedule_interval="0 1,13 * * *", # 01:00 and 13:00 every day
    catchup=False,
    tags=["Naya", "project", "final project", "Rotem Reich", "Yonatan Malihi", "Avi Yashar", "stores","api"]
)

def proccess_stores():



    @task()
    def drop_duplicates():
        daf.run_command("stores.py", "misc")

    
    (
    drop_duplicates()
    )

dag = proccess_stores()