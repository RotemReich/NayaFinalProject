from airflow.decorators import dag, task
import Base
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


@dag(
    dag_id = "transfer_to_parquet",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["Naya", "project", "final project", "Rotem Reich", "Yonatan Malihi", "Avi Yashar", "parquet", "transfer"]
)

def transfer_to_parquet():
    
    @task
    def load_shufersal_to_parquet():
        Base.run_command("shufersal_load.py", "spark")
    
    @task
    def wait_30_seconds_post_shufersal():
        Base.wait_command(30)

    @task
    def load_carrefour_to_parquet():
        Base.run_command("carrefour_load.py", "spark")
    
    @task
    def wait_30_seconds_post_carrefour():
        Base.wait_command(30)

    @task
    def load_ramilevi_to_parquet():
        Base.run_command("ramilevi_load.py", "spark")

    @task
    def wait_30_seconds_post_ramilevi():
        Base.wait_command(30)

    @task
    def load_victory_to_parquet():
        Base.run_command("victory_load.py", "spark")

    @task
    def wait_30_seconds_post_victory():
        Base.wait_command(30)
    
    @task
    def load_mahsaneihashuk_to_parquet():
        Base.run_command("mahsaneihashuk_load.py", "spark")

    @task
    def wait_30_seconds_post_mahsaneihashuk():
        Base.wait_command(30)

    Base.ensure_dag_unpaused("Crawlers_JSON")
    Crawlers_JSON_dag = TriggerDagRunOperator(
        task_id="Crawlers_JSON",
        trigger_dag_id="Crawlers_JSON",
        conf={"key": "value"},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    
    (
    load_shufersal_to_parquet() >> wait_30_seconds_post_shufersal() 
    >>
    load_carrefour_to_parquet() >> wait_30_seconds_post_carrefour()
    >>
    load_ramilevi_to_parquet() >> wait_30_seconds_post_ramilevi()
    >>
    load_victory_to_parquet() >> wait_30_seconds_post_victory()
    >>
    load_mahsaneihashuk_to_parquet() >> wait_30_seconds_post_mahsaneihashuk()
    >>
    Crawlers_JSON_dag
    )


dag = transfer_to_parquet()