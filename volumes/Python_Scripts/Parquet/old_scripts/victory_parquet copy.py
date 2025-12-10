from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import time
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

date_prefix = time.strftime("%Y-%m-%d") + "/"

S3_SOURCE_BUCKET = "naya-finalproject-sources/"
retailer_source_prefix = "victory-promofull-gz/"

S3_PROCESSED_BUCKET = "naya-finalproject-processed/"
PromotionDetails_prefix = "PromotionDetails/"
PromotionItems_prefix = "PromotionItems/"
retailer_prefix = "victory"

# Build SparkSession with Hadoop AWS and spark-xml
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("LoadVictoryXMLFromS3")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0")
    .config("spark.sql.files.ignoreMissingFiles", "true")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

# Ignore corrupt inputs at SQL layer to skip bad .gz files
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

# Read all XML.GZ files under subfolders (001/, 002/, ...) into one DF using glob and recursive lookup
s3_source_path = f"s3a://{S3_SOURCE_BUCKET}{date_prefix}{retailer_source_prefix}**/*.xml.gz"

# Each record is a <Sale> element inside <Sales>
row_tag = "Sale"

# Explicit schema based on your output
schema = T.StructType([
    T.StructField("AdditionalRestrictions", T.StringType(), True),
    T.StructField("AdditionalsCoupon", T.StringType(), True),
    T.StructField("AdditionalsGiftCount", T.DoubleType(), True),
    T.StructField("AdditionalsMinBasketAmount", T.StringType(), True),
    T.StructField("AdditionalsTotals", T.StringType(), True),
    T.StructField("AllowMultipleDiscounts", T.LongType(), True),
    T.StructField("ClubID", T.LongType(), True),
    T.StructField("DiscountRate", T.DoubleType(), True),
    T.StructField("DiscountType", T.LongType(), True),
    T.StructField("DiscountedPrice", T.DoubleType(), True),
    T.StructField("DiscountedPricePerMida", T.DoubleType(), True),
    T.StructField("IsGiftItem", T.LongType(), True),
    T.StructField("ItemCode", T.LongType(), True),
    T.StructField("ItemType", T.LongType(), True),
    T.StructField("MaxQty", T.DoubleType(), True),
    T.StructField("MinNoOfItemsOffered", T.LongType(), True),
    T.StructField("MinPurchaseAmount", T.DoubleType(), True),
    T.StructField("MinQty", T.DoubleType(), True),
    T.StructField("PriceUpdateDate", T.StringType(), True),
    T.StructField("PromotionDescription", T.StringType(), True),
    T.StructField("PromotionEndDate", T.StringType(), True),
    T.StructField("PromotionEndHour", T.StringType(), True),
    T.StructField("PromotionID", T.StringType(), True),
    T.StructField("PromotionStartDate", T.StringType(), True),
    T.StructField("PromotionStartHour", T.StringType(), True),
    T.StructField("Remarks", T.StringType(), True),
    T.StructField("RewardType", T.LongType(), True),
])

df = (
    spark.read.format("com.databricks.spark.xml")
    .option("rowTag", row_tag)
    .option("recursiveFileLookup", "true") # read all subfolders
    .option("ignoreNamespace", "true") # be resilient to XML namespace variations
    .schema(schema)
    .load(s3_source_path)
)

# Attach ChainID and StoreID from Promos-level metadata by joining on file path
# Keep rowTag=Sale for main DF; read again with rowTag=Promos to fetch file-level fields
promos_meta = (
    spark.read.format("com.databricks.spark.xml")
    .option("rowTag", "Promos")
    .option("recursiveFileLookup", "true")
    .load(s3_source_path)
    .select(
        F.input_file_name().alias("src"),
        F.col("ChainID").cast(T.StringType()).alias("ChainID"),
        F.col("StoreID").cast(T.StringType()).alias("StoreID"),
    )
    .dropDuplicates(["ChainID", "StoreID"])
    .withColumn("src", F.input_file_name())
)

df = (
    df.withColumn("src", F.input_file_name())
      .join(F.broadcast(promos_meta), on="src", how="left")
      .drop("src")
)

# pattern = r"/([0-9]{3})/[^/]+$"
# df = df.withColumn("BranchId", F.regexp_extract(F.input_file_name(), pattern, 1))
print("\n\n", f"Loaded rows: {df.count()}")
# df.printSchema()
# df.show(20, truncate=False)

df_PromotionDetails = (
    df.groupBy("ChainID", "StoreID", "PromotionID", "PromotionDescription",
               "PromotionStartDate", "PromotionEndDate", "MinQty", "MaxQty",
                "DiscountRate", "DiscountedPrice")
    .count()
    .withColumnRenamed("count", "NumOfProducts")
    .select(
        F.col("ChainID").cast(T.StringType()).alias("ChainID"),
        F.lpad(F.col("StoreID").cast(T.StringType()), 3, "0").alias("StoreID"),
        F.col("PromotionID").cast(T.StringType()).alias("PromotionID"),
        F.col("PromotionDescription").cast(T.StringType()).alias("PromotionDescription"),
        F.to_date(F.col("PromotionStartDate")).cast(T.DateType()).alias("PromotionStartDate"),
        F.to_date(F.col("PromotionEndDate")).cast(T.DateType()).alias("PromotionEndDate"),
        F.col("MinQty").cast(T.IntegerType()).alias("MinQty"),
        F.col("MaxQty").cast(T.IntegerType()).alias("MaxQty"),
        F.col("DiscountRate").cast(T.DoubleType()).alias("DiscountRate"),
        F.col("DiscountedPrice").cast(T.DoubleType()).alias("DiscountedPrice"),
        F.col("NumOfProducts").cast(T.IntegerType()).alias("NumOfProducts")
)
)
# df_PromotionDetails.printSchema()
# df_PromotionDetails.show(20, truncate=False)

s3_PromotionDetails_path = f"s3a://{S3_PROCESSED_BUCKET}{PromotionDetails_prefix}{date_prefix}{retailer_prefix}"
print("\n", f"Writing PromotionDetails to: {s3_PromotionDetails_path}", "\n")
df_PromotionDetails.write.parquet(s3_PromotionDetails_path, mode="overwrite")
print(f">>>{PromotionDetails_prefix} written successfully.")
# print(f"loaded {df_PromotionDetails.count()} rows.")

df_Items = (
    df
    .select(
        F.col("PromotionID").cast(T.StringType()),
        F.col("ItemCode").cast(T.StringType())
    )
    .dropDuplicates(["PromotionID", "ItemCode"])
)

s3_PromotionItems_path = f"s3a://{S3_PROCESSED_BUCKET}{PromotionItems_prefix}{date_prefix}{retailer_prefix}"
print("\n", f"Writing PromotionItems to: {s3_PromotionItems_path}", "\n")
df_Items.write.parquet(s3_PromotionItems_path, mode="overwrite")
print("\n", f">>>{PromotionItems_prefix} written successfully.")
# print(f"Loaded {df_Items.count()} rows.")

# Stop Spark when done
spark.stop()