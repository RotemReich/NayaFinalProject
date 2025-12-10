from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import subprocess
from datetime import datetime
import time
import os


log = LoggingMixin().log

@dag(
    dag_id = "transfer_to_parquet",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["Naya", "project", "final project", "Rotem Reich", "Yonatan Malihi", "Avi Yashar", "parquet", "transfer"]
)

def transfer_to_parquet():
    
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_token = os.getenv("AWS_SESSION_TOKEN")
    # course_broker = "course-kafka:9092"
    
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
    
    def wait_command(seconds=30):
        log.info(f">>>Waiting for {seconds} seconds...")
        time.sleep(seconds)
        log.info(f">>>{seconds} seconds are over!")

    @task
    def load_shufersal_to_parquet():
        run_command("shufersal_load.py", "dev_env")
    
    @task
    def wait_30_seconds_post_shufersal():
        wait_command(30)


    @task
    def load_ramilevi_to_parquet():
        run_command("ramilevi_load.py", "dev_env")
    
    @task
    def wait_30_seconds_post_ramilevi():
        wait_command(30)

    @task
    def load_victory_to_parquet():
        run_command("victory_load.py", "dev_env")
    
    @task
    def wait_30_seconds_post_victory():
        wait_command(30)

    @task
    def load_hazihinam_to_parquet():
        run_command("hazihinam_load.py", "dev_env")
    
    @task
    def wait_30_seconds_post_hazihinam():
        wait_command(30)
    
    run_aws_glue_dag = TriggerDagRunOperator(
        task_id="run_aws_glue",
        trigger_dag_id="run_aws_glue",
        conf={"key": "value"},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    
    (
    load_shufersal_to_parquet() >> wait_30_seconds_post_shufersal() 
    >>
    load_ramilevi_to_parquet() >> wait_30_seconds_post_ramilevi()
    >>
    load_victory_to_parquet() >> wait_30_seconds_post_victory()
    >>
    load_hazihinam_to_parquet() >> wait_30_seconds_post_hazihinam()
    >>
    run_aws_glue_dag
    )


dag = transfer_to_parquet()