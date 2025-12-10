import os
import time
import boto3

"""
Run an AWS Glue Crawler and wait for completion.
- Credentials and region are taken from environment variables:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN (optional), AWS_DEFAULT_REGION
- Configure CRAWLER_NAME via env: GLUE_CRAWLER_NAME (default: PromotionDetailsCrawler)
"""

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

class GlueCrawler:
    def __init__(self, crawler_name):
        self.crawler_name = crawler_name
        self.region = REGION
        self.aws_access_key_id = AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = AWS_SECRET_ACCESS_KEY

        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise RuntimeError("Missing AWS credentials in environment")
        
        self.glue_client = self.create_glue_client()
        print(f"Starting crawler: {self.crawler_name} in region {self.region}")

    def create_glue_client(self):
        return boto3.client(
                "glue",
                region_name=self.region,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
        
    def run_crawler(self):
        # Try to start the crawler (it might already be running)
        try:
            self.glue_client.start_crawler(Name=self.crawler_name)
            print("Crawler start requested.")
            # Poll status until READY
            while True:
                resp = self.glue_client.get_crawler(Name=self.crawler_name)
                state = resp["Crawler"]["State"]  # RUNNING | STOPPING | READY
                last_run = resp["Crawler"].get("LastCrawl")
                print(f"Crawler state: {state}. LastCrawl: {last_run}")

                if state == "READY":
                    print("Crawler completed (READY).")
                    resp = self.glue_client.get_crawler_metrics(CrawlerNameList=[self.crawler_name])
                    print("Crawler metrics:", resp)
                    break
                time.sleep(15)  # wait before checking again
        except self.glue_client.exceptions.CrawlerRunningException:
            print("Crawler is already running.")
        except self.glue_client.exceptions.EntityNotFoundException:
            raise RuntimeError(f"Crawler '{self.crawler_name}' not found.")




    

# Optional: print last crawl status info

