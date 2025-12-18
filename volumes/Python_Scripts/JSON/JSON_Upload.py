from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import time
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

S3_PROCESSED_BUCKET = "naya-finalproject-processed"
date_prefix = time.strftime("%Y-%m-%d")

S3_JSON_BUCKET = "naya-finalproject-json"


# Build SparkSession with Hadoop AWS
spark = (
    SparkSession.builder
    .appName("LoadProcessedDataFromS3")
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
s3_PromotionDetails_path = f"s3a://{S3_PROCESSED_BUCKET}/PromotionDetails/KeyDate={date_prefix}/**/*.parquet"

# Explicit schema based on your output
PromotionDetails_schema = T.StructType([
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
    T.StructField("NumOfProducts", T.IntegerType(), True)
])
    
PromotionDetails_df = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .schema(PromotionDetails_schema)
    .load(s3_PromotionDetails_path)        
)
# print("\n\n", f"Loaded rows: {PromotionDetails_df.count()}")
# df.printSchema()
# df.show(20, truncate=False)


s3_PromotionItems_path = f"s3a://{S3_PROCESSED_BUCKET}/PromotionItems/KeyDate={date_prefix}/**/*.parquet"
# Explicit schema based on your output
PromotionItems_schema = T.StructType([
    T.StructField("ChainID", T.StringType(), True),
    T.StructField("StoreID", T.StringType(), True),
    T.StructField("PromotionId", T.StringType(), True),
    T.StructField("ItemCode", T.StringType(), True),
    T.StructField("itemprice", T.DoubleType(), True)
])

PromotionItems_df = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .schema(PromotionItems_schema)
    .load(s3_PromotionItems_path)
)
# print("\n\n", f"Loaded rows: {PromotionItems_df.count()}")
# PromotionItems_df.printSchema()
# PromotionItems_df.show(20, truncate=False)

PromotionItems_agg_df = (
    PromotionItems_df
    .groupBy("ChainID", "StoreID", "PromotionId")
    .agg((F.sum("itemprice") / F.count("ItemCode")).alias("AvgItemPrice"))
)


s3_Stores_path = f"s3a://{S3_PROCESSED_BUCKET}/stores/stores_output_with_latlon.ndjson"
Stores_schema = T.StructType([
    T.StructField("ChainID", T.StringType(), True),
    T.StructField("SubChainID", T.StringType(), True),
    T.StructField("StoreID", T.StringType(), True),
    T.StructField("ChainName", T.StringType(), True),
    T.StructField("SubChainName", T.StringType(), True),
    T.StructField("StoreName", T.StringType(), True),
    T.StructField("Address", T.StringType(), True),
    T.StructField("City", T.StringType(), True),
    T.StructField("Latitude", T.DoubleType(), True),
    T.StructField("Longitude", T.DoubleType(), True)
])

Stores_df = (
    spark.read.format("json")
    # .option("recursiveFileLookup", "true")
    .schema(Stores_schema)
    .load(s3_Stores_path)
)
# print("\n\n", f"Loaded rows: {Stores_df.count()}")
# Stores_df.printSchema()
# Stores_df.show(20, truncate=False)

# Join PromotionDetails with PromotionItems average price and Stores info
df = (
    PromotionDetails_df
    .join(PromotionItems_agg_df, (PromotionDetails_df["ChainID"] == PromotionItems_agg_df["ChainID"]) &
                                   (PromotionDetails_df["StoreID"] == PromotionItems_agg_df["StoreID"]) &
                                   (PromotionDetails_df["PromotionID"] == PromotionItems_agg_df["PromotionId"]),
                                   how="inner")
    .join(Stores_df, on=["ChainID", "StoreID"], how="left")
    .withColumn("DiscountRate_calc",
                F.when(PromotionDetails_df["DiscountRate"]<1, PromotionDetails_df["DiscountRate"])
                .when((PromotionDetails_df["DiscountRate"]>=1) & (PromotionDetails_df["DiscountedPrice"].isNull()), 1 - (PromotionDetails_df["DiscountRate"]/(F.col("AvgItemPrice")*PromotionDetails_df["MinQty"])))
                .otherwise(1 - (PromotionDetails_df["DiscountedPrice"]/(F.col("AvgItemPrice")*PromotionDetails_df["MinQty"])))
                .cast(T.DoubleType())
    )
    .withColumn("DiscountedPrice_calc",
                F.when(PromotionDetails_df["DiscountedPrice"].isNull(), F.col("DiscountRate_calc")*F.col("AvgItemPrice")*PromotionDetails_df["MinQty"])
                .otherwise(PromotionDetails_df["DiscountedPrice"]).cast(T.DoubleType())
    )
    .withColumn("MagicNumber",
                ((3/PromotionDetails_df["MinQty"])
                + (F.col("DiscountRate_calc")*5)
                + F.col("DiscountedPrice_calc")
                ).cast(T.DoubleType()))
    .select(
        PromotionDetails_df["PromotionID"].cast(T.StringType()).alias("PromotionID"),
        PromotionDetails_df["PromotionDescription"].cast(T.StringType()).alias("PromotionDescription"),
        PromotionDetails_df["NumOfProducts"].cast(T.IntegerType()).alias("NumOfProducts"),
        F.col("AvgItemPrice").cast(T.DoubleType()).alias("AvgItemPrice"),
        F.col("DiscountRate_calc").cast(T.DoubleType()).alias("DiscountRate"),
        F.col("DiscountedPrice_calc").cast(T.DoubleType()).alias("DiscountedPrice"),
        F.col("MagicNumber").cast(T.DoubleType()).alias("MagicNumber"),
        PromotionDetails_df["MinQty"].cast(T.IntegerType()).alias("MinQty"),
        PromotionDetails_df["MaxQty"].cast(T.IntegerType()).alias("MaxQty"),
        PromotionDetails_df["PromotionStartDate"].cast(T.DateType()).alias("PromotionStartDate"),
        PromotionDetails_df["PromotionEndDate"].cast(T.DateType()).alias("PromotionEndDate"),
        PromotionDetails_df["ChainID"].cast(T.StringType()).alias("ChainID"),
        PromotionDetails_df["ChainNameHeb"].cast(T.StringType()).alias("ChainNameHeb"),
        PromotionDetails_df["ChainNameEng"].cast(T.StringType()).alias("ChainNameEng"),
        Stores_df["StoreID"].cast(T.StringType()).alias("StoreID"),
        Stores_df["StoreName"].cast(T.StringType()).alias("StoreName"),
        Stores_df["Address"].cast(T.StringType()).alias("Address"),
        Stores_df["City"].cast(T.StringType()).alias("City"),
        Stores_df["Latitude"].cast(T.DoubleType()).alias("Latitude"),
        Stores_df["Longitude"].cast(T.DoubleType()).alias("Longitude")
    )
)



# Write JSON to S3 using Spark (writes a directory with part files)
json_prefix = f"s3a://{S3_JSON_BUCKET}/PromotionDetails_json"
# Optionally reduce small files
# df.coalesce(8).write.mode("overwrite").json(json_prefix)
df.write.mode("overwrite").json(json_prefix)
print(f"Wrote JSON part files under {json_prefix}")

spark.stop()