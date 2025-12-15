import glue_crawler
import os

CRAWLER_NAME = os.getenv("GLUE_CRAWLER_NAME", "stores-crawler")

if __name__ == "__main__":
    crawler = glue_crawler.GlueCrawler(CRAWLER_NAME)
    crawler.run_crawler()
