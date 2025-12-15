from airflow.decorators import dag, task
import Base
from datetime import datetime


@dag(
    dag_id = "run_aws_glue",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["Naya", "project", "final project", "Rotem Reich", "Yonatan Malihi", "Avi Yashar", "glue", "crawler"]
)

def run_aws_glue():
        

    @task
    def run_crawler_promotiondetails():
        Base.run_command("glue_crawler_PromotionDetails.py", "glue")
    @task
    def run_crawler_promotionitems():
        Base.run_command("glue_crawler_PromotionItems.py", "glue")
        
    @task
    def run_crawler_stores():
        Base.run_command("glue_crawler_stores.py", "glue")

    # @task
    # def run_glue_job_JSON():
    #     Base.run_command("glue_job_JSON.py", "glue")

    # @task
    # def delete_json():
    #     Base.delete_json_from_s3("naya-finalproject-json", "PromotionDetails_json/")
        
    @task
    def run_spark_JSON():
        Base.run_command("JSON_Upload.py", "JSON")

    (
        run_crawler_promotiondetails()
        >>
        run_crawler_promotionitems()
        >>
        run_crawler_stores()
        >>
        # delete_json() >> run_glue_job_JSON()
        run_spark_JSON()
    )


dag = run_aws_glue()