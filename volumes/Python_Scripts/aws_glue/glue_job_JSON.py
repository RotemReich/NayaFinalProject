import boto3
import time
import os

# === AWS Credentials ===
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Create Glue client
glue_client = boto3.client(
    'glue',
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Start the job
response = glue_client.start_job_run(
    JobName='Load JSON',
    # Arguments={
    #     '--final-project-source-date': '2025-12-08',  # or dynamically set
    #     '--TARGET_PATH': 's3://my-bucket/output.json'
    # }
)

print("Job started with run ID:", response['JobRunId'])

job_run_id = response['JobRunId']

while True:
    status_response = glue_client.get_job_run(JobName='Load JSON', RunId=job_run_id)
    status = status_response['JobRun']['JobRunState']
    print("Current job status:", status)
    

    if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
        print("Job status:", status)
        break
    time.sleep(15)  # wait before checking again
