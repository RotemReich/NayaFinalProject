from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import subprocess
from datetime import datetime
import time
import os


log = LoggingMixin().log

@dag(
    dag_id = "run_aws_glue",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["Naya", "project", "final project", "Rotem Reich", "Yonatan Malihi", "Avi Yashar", "glue", "crawler"]
)

def run_aws_glue():
    
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    def run_command(file, container):
        base_cmd = f"docker exec"
        container = "python_scripts"
        python_path = "/usr/local/bin/python3.13"
        path = "/home/developer/final_project/aws_glue/"
        
        if not aws_access_key or not aws_secret_key:
            raise ValueError("Missing AWS credentials in environment")
        else:
            base_cmd += f" -e AWS_ACCESS_KEY_ID={aws_access_key} -e AWS_SECRET_ACCESS_KEY={aws_secret_key}"

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
    
    @task
    def run_glue_job_JSON():
        run_command("glue_job_JSON.py", "dev_env")
        

    @task
    def run_crawler_promotiondetails():
        run_command("glue_crawler_PromotionDetails.py", "dev_env")


    (
        run_glue_job_JSON()
        >> run_crawler_promotiondetails()
    )


dag = run_aws_glue()