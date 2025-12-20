from airflow.utils.log.logging_mixin import LoggingMixin
import subprocess
import time
import os
from airflow.models import DagModel
from airflow.utils.session import create_session
import boto3


log = LoggingMixin().log


aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_token = os.getenv("AWS_SESSION_TOKEN")
    
def run_command(file, process):
    base_cmd = f"docker exec"

    if process in ["download", "glue", "misc"]:
        container = "python_scripts"
        python_path = "/usr/local/bin/python3.13"
        if process == "glue":
            path = "/home/developer/final_project/aws_glue/"
        elif process == "download":
            path = "/home/developer/final_project/Sources/"
        elif process == "misc":
            path = "/home/developer/final_project/miscellaneous/"
    elif process == "spark":
        container = "dev_env"
        python_path = "/usr/bin/python3"
        path = "/home/developer/projects/final_project/python_scripts/Parquet/"
    elif process == "JSON":
        container = "dev_env"
        python_path = "/usr/bin/python3"
        path = "/home/developer/projects/final_project/python_scripts/JSON/"
    elif process == "JSON_to_elastic":
        container = "python_scripts"
        python_path = "/usr/local/bin/python3.13"
        path = "/home/developer/projects/final_project/python_scripts/JSON/"
    else:
        raise ValueError("Invalid process name")
    
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

def ensure_dag_unpaused(dag_id):
    with create_session() as session:
        dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
        if dag_model and dag_model.is_paused:
            dag_model.is_paused = False
            session.commit()
            log.info(f"DAG '{dag_id}' was paused. Now unpaused.")

def delete_json_from_s3(bucket_name, prefix):
   s3 = boto3.resource("s3")
   bucket = s3.Bucket(bucket_name)
   bucket.objects.filter(Prefix=prefix).delete()
   return f"All objects in prefix '{prefix}' deleted from bucket '{bucket_name}'."
