from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import time
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

date_prefix = time.strftime("%Y-%m-%d") + "/"

S3_SOURCE_BUCKET = "naya-finalproject-sources/"
retailer_source_prefix = "shufersal-promofull-gz/"

S3_PROCESSED_BUCKET = "naya-finalproject-processed/"
PromotionDetails_prefix = "PromotionDetails/"
PromotionItems_prefix = "PromotionItems/"
retailer_prefix = "shufersal"

# Build SparkSession with Hadoop AWS and spark-xml
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("LoadShufersalXMLFromS3")
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
s3_source_path = f"s3a://{S3_SOURCE_BUCKET}{date_prefix}{retailer_source_prefix}**/*.gz"

# Each record is a <Promotion> element inside <Promotions>
row_tag = "Promotion"

# Schema aligned to the shown XML
promotion_item_schema = T.StructType([
    T.StructField("ItemCode", T.StringType(), True),
    T.StructField("ItemType", T.IntegerType(), True),
    T.StructField("IsGiftItem", T.IntegerType(), True),
])

additional_restrictions_schema = T.StructType([
    T.StructField("AdditionalIsCoupon", T.IntegerType(), True),
    T.StructField("AdditionalGiftCount", T.IntegerType(), True),
    T.StructField("AdditionalIsTotal", T.IntegerType(), True),
    T.StructField("AdditionalIsActive", T.IntegerType(), True),
])

clubs_schema = T.StructType([
    T.StructField("ClubId", T.IntegerType(), True),
])

schema = T.StructType([
    T.StructField("PromotionId", T.StringType(), True),
    T.StructField("AllowMultipleDiscounts", T.IntegerType(), True),
    T.StructField("PromotionDescription", T.StringType(), True),
    T.StructField("PromotionUpdateDate", T.StringType(), True),
    T.StructField("PromotionStartDate", T.StringType(), True),
    T.StructField("PromotionStartHour", T.StringType(), True),
    T.StructField("PromotionEndDate", T.StringType(), True),
    T.StructField("PromotionEndHour", T.StringType(), True),
    T.StructField("IsWeightedPromo", T.IntegerType(), True),
    T.StructField("MinQty", T.DoubleType(), True),
    T.StructField("MaxQty", T.DoubleType(), True),
    T.StructField("DiscountType", T.IntegerType(), True),
    T.StructField("RewardType", T.IntegerType(), True),
    T.StructField("DiscountRate", T.DoubleType(), True),
    T.StructField("DiscountedPrice", T.DoubleType(), True),
    T.StructField("MinNoOfItemOfered", T.IntegerType(), True),
    # Nested arrays/structs
    T.StructField("PromotionItems", T.StructType([
        # Include attribute field for Count with prefix '_'
        T.StructField("_Count", T.IntegerType(), True),
        T.StructField("Item", T.ArrayType(promotion_item_schema), True)
    ]), True),
    T.StructField("AdditionalRestrictions", additional_restrictions_schema, True),
    T.StructField("Clubs", T.ArrayType(clubs_schema), True),
])

# Read with attributePrefix to expose attributes (e.g., PromotionItems._Count)
df = (
    spark.read.format("com.databricks.spark.xml")
    .option("rowTag", row_tag)
    .option("recursiveFileLookup", "true")
    .option("attributePrefix", "_")
    .option("ignoreNamespace", "true")
    .schema(schema)
    .load(s3_source_path)
)

header_schema = T.StructType([
    T.StructField("ChainId", T.StringType(), True),
    T.StructField("SubChainId", T.StringType(), True),
    T.StructField("StoreId", T.StringType(), True),
    T.StructField("BikoretNo", T.IntegerType(), True),
    T.StructField("DllVerNo", T.StringType(), True)
])

root_meta = (
    spark.read.format("com.databricks.spark.xml")
    .option("rowTag", "root")
    .option("recursiveFileLookup", "true")
    .schema(header_schema)
    .load(s3_source_path)
    .select(
        F.input_file_name().alias("src"),
        F.col("ChainId").cast(T.StringType()).alias("ChainId"),
        F.col("StoreId").cast(T.StringType()).alias("StoreId"),
    )
    .dropDuplicates(["ChainId", "StoreId"])
    .withColumn("src", F.input_file_name())
)

# root_meta.printSchema()
# root_meta.show(5, truncate=False)

df = (
    df.withColumn("src", F.input_file_name())
      .join(F.broadcast(root_meta), on="src", how="left")
      .drop("src")
)

# Add NumOfProducts and select promotion-level fields
df_PromotionDetails = (
    df
    .withColumn("NumOfProducts", F.col("PromotionItems._Count").cast(T.IntegerType()))
    .select(
        F.col("ChainId").cast(T.StringType()).alias("ChainID"),
        F.lpad(F.col("StoreId").cast(T.StringType()), 3, "0").alias("StoreID"),
        F.col("PromotionId").cast(T.StringType()).alias("PromotionID"),
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
# df_PromotionDetails.show(5, truncate=False)

s3_PromotionDetails_path = f"s3a://{S3_PROCESSED_BUCKET}{PromotionDetails_prefix}{date_prefix}{retailer_prefix}"
print("\n\n", f"Writing PromotionDetails to: {s3_PromotionDetails_path}")
df_PromotionDetails.write.parquet(s3_PromotionDetails_path, mode="overwrite")
print(f">>>{PromotionDetails_prefix} written successfully.\nloaded {df_PromotionDetails.count()} rows.")

# Extract PromotionItems as a second DataFrame keyed by PromotionId
df_items = (
    df
    .select(
        F.col("PromotionId").alias("PromotionID"),
        F.explode_outer(F.col("PromotionItems.Item")).alias("Item")
    )
    .select(
        F.col("PromotionID").cast(T.StringType()).alias("PromotionID"),
        F.col("Item.ItemCode").cast(T.StringType()).alias("ItemCode")
    )
    .dropDuplicates(["PromotionID", "ItemCode"])
)

s3_PromotionItems_path = f"s3a://{S3_PROCESSED_BUCKET}{PromotionItems_prefix}{date_prefix}{retailer_prefix}"
print("\n\n", f"Writing PromotionItems to: {s3_PromotionItems_path}")
df_items.write.parquet(s3_PromotionItems_path, mode="overwrite")
print(f">>>{PromotionItems_prefix} written successfully.")

spark.stop()