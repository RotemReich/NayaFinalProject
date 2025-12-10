from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import time
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

S3_PROCESSED_BUCKET = "naya-finalproject-processed/"
date_prefix = time.strftime("%Y-%m-%d") + "/"

S3_JSON_BUCKET = "naya-finalproject-json"


# Build SparkSession with Hadoop AWS
spark = (
    SparkSession.builder
    .appName("LoadParquetFromS3")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    # Remove ManifestCommitter configs to avoid CNF; use defaults
    # .config("spark.hadoop.mapreduce.outputcommitter.factory.class", "org.apache.hadoop.mapreduce.lib.output.committer.ManifestCommitterFactory")
    # .config("spark.hadoop.fs.s3a.committer.enable", "true")
    # .config("spark.hadoop.fs.s3a.committer.name", "directory")
    # .config("spark.hadoop.fs.s3a.committer.magic.enabled", "false")
    # .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
    # .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/s3a-staging")
    .config("spark.sql.files.ignoreMissingFiles", "true")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

# Load all Parquet under PromotionDetails/{date}/{retailer}/ by pointing to the parent directories
# Spark will discover part files with recursiveFileLookup
s3_processed_path = f"s3a://{S3_PROCESSED_BUCKET}PromotionDetails/{date_prefix}*/"

# Explicit schema based on your output
schema = T.StructType([
    T.StructField("ChainID", T.StringType(), True),
    T.StructField("ChainNameHeb", T.StringType(), True),
    T.StructField("ChainNameEng", T.StringType(), True),
    T.StructField("StoreID", T.StringType(), True),
    T.StructField("PromotionID", T.StringType(), True),
    T.StructField("PromotionDescription", T.StringType(), True),
    T.StructField("PromotionStartDate", T.DateType(), True),
    T.StructField("PromotionEndDate", T.DateType(), True),
    T.StructField("MinQty", T.IntegerType(), True),
    T.StructField("MaxQty", T.IntegerType(), True),
    T.StructField("DiscountRate", T.DoubleType(), True),
    T.StructField("DiscountedPrice", T.DoubleType(), True),
    T.StructField("NumOfProducts", T.IntegerType(), True),
    T.StructField("KeyDate", T.StringType(), True),
    T.StructField("ChainPrefix", T.StringType(), True)
])

df = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .schema(schema)
    .load(s3_processed_path)
    .select(
        F.col("ChainID").cast(T.StringType()).alias("ChainID"),
        F.col("ChainNameHeb").cast(T.StringType()).alias("ChainNameHeb"),
        F.col("ChainNameEng").cast(T.StringType()).alias("ChainNameEng"),
        F.col("StoreID").cast(T.StringType()).alias("StoreID"),
        F.col("PromotionID").cast(T.StringType()).alias("PromotionID"),
        F.col("PromotionDescription").cast(T.StringType()).alias("PromotionDescription"),
        F.col("PromotionStartDate").cast(T.DateType()).alias("PromotionStartDate"),
        F.col("PromotionEndDate").cast(T.DateType()).alias("PromotionEndDate"),
        F.col("MinQty").cast(T.IntegerType()).alias("MinQty"),
        F.col("MaxQty").cast(T.IntegerType()).alias("MaxQty"),
        F.col("DiscountRate").cast(T.DoubleType()).alias("DiscountRate"),
        F.col("DiscountedPrice").cast(T.DoubleType()).alias("DiscountedPrice"),
        F.col("NumOfProducts").cast(T.IntegerType()).alias("NumOfProducts")
    )
)
print("\n\n", f"Loaded rows: {df.count()}")

# df.printSchema()
# df.show(20, truncate=False)

# Write JSON to S3 using Spark (writes a directory with part files)
json_prefix = f"s3a://{S3_JSON_BUCKET}/PromotionDetails_json"
# Optionally reduce small files
# df.coalesce(8).write.mode("overwrite").json(json_prefix)
df.write.mode("overwrite").json(json_prefix)
print(f"Wrote JSON part files under {json_prefix}")

spark.stop()