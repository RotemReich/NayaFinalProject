from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import subprocess
from datetime import datetime, timedelta
import time
import os


log = LoggingMixin().log

@dag(
    dag_id = "download_daily_files",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["Naya", "project", "final project", "Rotem Reich", "Yonatan Malihi", "Avi Yashar", "download"]
)

def download_daily_files():
    
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_token = os.getenv("AWS_SESSION_TOKEN")
    
    def run_command(file, container):
        base_cmd = f"docker exec"
        
        if container == "python_scripts":
            python_path = "/usr/local/bin/python3.13"
            path = "/home/developer/final_project/Sources/"
        elif container == "dev_env":
            python_path = "/usr/bin/python3"
            path = "/home/developer/projects/final_project/python_scripts/Parquet/"
        else:
            raise ValueError("Invalid container name")
        
        if not aws_access_key or not aws_secret_key:
            raise ValueError("Missing AWS credentials in environment")
        else:
            base_cmd += f" -e AWS_ACCESS_KEY_ID={aws_access_key} -e AWS_SECRET_ACCESS_KEY={aws_secret_key}"

        if aws_token:
            base_cmd += f" -e AWS_SESSION_TOKEN={aws_token}"

        command = f"{base_cmd} {container} {python_path} {path}{file}"
              
        try:
            result = subprocess.run(command ,shell=True, check=True, capture_output=True, text=True)
            log.info("STDOUT: %s", result.stdout)
            log.info("STDERR: %s", result.stderr)
            return f"'{file}' Script executed successfully"
        except subprocess.CalledProcessError as e:
            log.error("Error running '%s': returncode=%s", file, e.returncode)
            log.error("STDOUT:\n%s", e.stdout)
            log.error("STDERR:\n%s", e.stderr)
            raise
    
    def wait_command(seconds=10):
        log.info(f">>>Waiting for {seconds} seconds...")
        time.sleep(seconds)
        log.info(f">>>{seconds} seconds are over!")

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def wait_10_seconds():
        wait_command(10)

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def download_shufersal():
        run_command("Shufersal_Upload_S3.py", "python_scripts")

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def download_ramilevi():
        run_command("RamiLevi_Upload_S3.py", "python_scripts")

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def download_victory():
        run_command("Victory_Upload_S3.py", "python_scripts")

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def download_hazihinam():
        run_command("HatziHinam_Upload_S3.py", "python_scripts")

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=30))
    def corrupt_handling():
        run_command("corrupt_handling/fix_corrupt_gz.py", "python_scripts")
    
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
    [download_shufersal(), download_ramilevi(), download_victory(), download_hazihinam()]
    >> wait_10_seconds()
    >> [corrupt_handling()]
    >> trigger_transfer_to_parquet_dag
    
    # download_ramilevi() >>
    # download_hazihinam() >>
    # download_victory() >>
    # download_shufersal() >>
    # corrupt_handling() >>
    # trigger_transfer_to_parquet_dag
    )

dag = download_daily_files()