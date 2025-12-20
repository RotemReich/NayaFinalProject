from airflow.decorators import dag, task
import dag_assist_functions as daf
from datetime import datetime


@dag(
    dag_id = "Crawlers_JSON",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["Naya", "project", "final project", "Rotem Reich", "Yonatan Malihi", "Avi Yashar", "glue", "crawler"]
)

def Crawlers_JSON():        

    @task
    def run_crawler_promotiondetails():
        daf.run_command("glue_crawler_PromotionDetails.py", "glue")
    @task
    def run_crawler_promotionitems():
        daf.run_command("glue_crawler_PromotionItems.py", "glue")

    @task
    def run_crawler_stores():
        daf.run_command("glue_crawler_stores.py", "glue")

    @task
    def run_spark_JSON():
        daf.run_command("JSON_Upload.py", "JSON")

    @task
    def json_to_elastic():
        daf.run_command("JSON_to_elastic.py", "JSON_to_elastic")

    (
        [run_crawler_promotiondetails(), run_crawler_promotionitems(), run_crawler_stores()]
        >>
        run_spark_JSON()
        >>
        json_to_elastic()
    )


dag = Crawlers_JSON()