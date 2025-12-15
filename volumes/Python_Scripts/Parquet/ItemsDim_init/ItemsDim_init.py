from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os
import time

AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
today_prefix = time.strftime("%Y-%m-%d")
bucket_name="naya-finalproject-sources"

s3_source_path = f"s3a://{bucket_name}/{today_prefix}/PriceFull/"  # Use folder only, recursiveFileLookup will find all files

spark = (
        SparkSession
        .builder
        .master("local[*]")
        .appName("LoadShufersalXMLFromS3")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.sql.files.openCostInBytes", "134217728")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
        )

items_schema = T.StructType([
    T.StructField("PriceUpdateDate", T.StringType(), True),
    T.StructField("ItemCode", T.StringType(), True),
    T.StructField("ItemType", T.IntegerType(), True),
    T.StructField("ItemName", T.StringType(), True),
    T.StructField("ManufacturerName", T.StringType(), True),
    T.StructField("ManufactureCountry", T.StringType(), True),
    T.StructField("ManufacturerItemDescription", T.StringType(), True),
    T.StructField("UnitQty", T.StringType(), True),
    T.StructField("Quantity", T.DoubleType(), True),
    T.StructField("UnitOfMeasure", T.StringType(), True),
    T.StructField("bIsWeighted", T.IntegerType(), True),
    T.StructField("QtyInPackage", T.StringType(), True),
    T.StructField("ItemPrice", T.DoubleType(), True),
    T.StructField("UnitOfMeasurePrice", T.DoubleType(), True),
    T.StructField("AllowDiscount", T.IntegerType(), True),
    T.StructField("ItemStatus", T.IntegerType(), True),
    T.StructField("ItemId", T.StringType(), True),
])

df = (
    spark
    .read
    .format("com.databricks.spark.xml")
    .option("rowTag", "Item")
    .option("recursiveFileLookup", "true")
    .option("attributePrefix", "_")
    .option("ignoreNamespace", "true")
    .schema(items_schema)
    .load(s3_source_path)
)
print("PriceFull raw:")
df.show(5, truncate=False)

df_thinned = (
    df
    .select(
        F.col("ItemCode").cast(T.StringType()).alias("ItemCode"),
        F.col("ItemName").cast(T.StringType()).alias("ItemName"),
        F.col("ManufacturerName").cast(T.StringType()).alias("ManufacturerName"),
        F.col("ManufactureCountry").cast(T.StringType()).alias("ManufactureCountry"),
        F.col("UnitQty").cast(T.StringType()).alias("UnitQty"),
        F.col("Quantity").cast(T.DoubleType()).alias("Quantity"),
        F.col("QtyInPackage").cast(T.StringType()).alias("QtyInPackage")
    )
    .dropDuplicates()
)
print("PriceFull thinned:")
df_thinned.printSchema()
df_thinned.show(5, truncate=False)

df_filtered = (
    df_thinned
    .filter(~F.lower(F.coalesce(F.trim(F.col("ItemName")), F.lit("xxx"))).isin("xxx", "unknown", "לא ידוע"))
)
print("PriceFull filtered:")
df_filtered.show(5, truncate=False)
df_filtered.printSchema()

df_final = (
    df_filtered    
    .orderBy(
    F.when(
        F.lower(F.coalesce(F.trim(F.col("ItemName")), F.lit("xxx")))
        .isin("xxx", "unknown", "לא ידוע"), 0).otherwise(F.length(F.col("ItemName"))), ascending=False
    )
    .orderBy(
    F.when(
        F.lower(F.coalesce(F.trim(F.col("ManufacturerName")), F.lit("xxx")))
        .isin("xxx", "unknown", "לא ידוע"), 0).otherwise(F.length(F.col("ManufacturerName"))), ascending=False
    )
    .orderBy(
    F.when(
        F.lower(F.coalesce(F.trim(F.col("ManufactureCountry")), F.lit("xxx")))
        .isin("xxx", "unknown", "לא ידוע"), 0).otherwise(F.length(F.col("ManufactureCountry"))), ascending=False
    )
    .orderBy(
    F.when(
        F.lower(F.coalesce(F.trim(F.col("UnitQty")), F.lit("xxx")))
        .isin("xxx", "unknown", "לא ידוע"), 0).otherwise(F.length(F.col("UnitQty"))), ascending=False
    )
    .orderBy(F.col("Quantity").desc())
    .orderBy(F.col("QtyInPackage").desc())
    .groupBy("ItemCode")
    .agg(
        F.first("ItemName").alias("ItemName"),
        F.first("ManufacturerName").alias("ManufacturerName"),
        F.first("ManufactureCountry").alias("ManufactureCountry"),
        F.first("UnitQty").alias("UnitQty"),
        F.first("Quantity").alias("Quantity"),
        F.first("QtyInPackage").alias("QtyInPackage")
    )
)
print("PriceFull final:")
df_final.printSchema()
df_final.show(5, truncate=False)

df_final.write.mode("overwrite").parquet(f"s3a://naya-finalproject-processed/ItemsDim/")