from airflow.decorators import dag, task
import Base
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


@dag(
    dag_id = "download_daily_files",
    start_date=datetime(2025, 1, 1),
    # schedule_interval=None,
    schedule_interval="0 1,13 * * *", # 01:00 and 13:00 every day
    catchup=False,
    tags=["Naya", "project", "final project", "Rotem Reich", "Yonatan Malihi", "Avi Yashar", "download"]
)

def download_daily_files():

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def Shufersal_PromoFull():
        Base.run_command("Shufersal_PromoFull_S3.py", "download")
    
    @task()
    def wait_30_seconds_post_Shufersal_PromoFull():
        Base.wait_command(30)

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def Shufersal_PriceFull():
        Base.run_command("Shufersal_PriceFull_S3.py", "download")



    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def Carrefour_PromoFull():
        Base.run_command("Carrefour_PromoFull_S3.py", "download")
    
    @task()
    def wait_30_seconds_post_Carrefour_PromoFull():
        Base.wait_command(30)

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def Carrefour_PriceFull():
        Base.run_command("Carrefour_PriceFull_S3.py", "download")



    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def RamiLevi_PromoFull():
        Base.run_command("RamiLevi_PromoFull_S3.py", "download")
    
    @task()
    def wait_30_seconds_post_RamiLevi_PromoFull():
        Base.wait_command(30)

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def RamiLevi_PriceFull():
        Base.run_command("RamiLevi_PriceFull_S3.py", "download")



    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def Victory_PromoFull():
        Base.run_command("Victory_PromoFull_S3.py", "download")
    
    @task()
    def wait_30_seconds_post_Victory_PromoFull():
        Base.wait_command(30)

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def Victory_PriceFull():
        Base.run_command("Victory_PriceFull_S3.py", "download")



    @task()
    def wait_10_seconds():
        Base.wait_command(10)

    @task()
    def corrupt_handling():
        Base.run_command("fix_corrupt_gz.py", "misc")

    @task()
    def drop_duplicates():
        Base.run_command("drop_duplicates.py", "misc")

    # Unpause transfer_to_parquet before triggering
    Base.ensure_dag_unpaused("transfer_to_parquet")
    trigger_transfer_to_parquet_dag = TriggerDagRunOperator(
        task_id="trigger_transfer_to_parquet_dag",
        trigger_dag_id="transfer_to_parquet",
        conf={"key": "value"},
        # Wait until downstream DAG finishes before marking this task done
        wait_for_completion=False,
        # Optional: reduce queued cases by giving a run_id and reset default conf
        reset_dag_run=True,
    )


    (
    [
        Shufersal_PromoFull() >> wait_30_seconds_post_Shufersal_PromoFull() >> Shufersal_PriceFull(),
        Carrefour_PromoFull() >> wait_30_seconds_post_Carrefour_PromoFull() >> Carrefour_PriceFull(),
        RamiLevi_PromoFull() >> wait_30_seconds_post_RamiLevi_PromoFull() >> RamiLevi_PriceFull(),
        Victory_PromoFull() >> wait_30_seconds_post_Victory_PromoFull() >> Victory_PriceFull()
    ]
    >> wait_10_seconds()
    >> corrupt_handling()
    >> drop_duplicates()
    >> trigger_transfer_to_parquet_dag
    )

dag = download_daily_files()