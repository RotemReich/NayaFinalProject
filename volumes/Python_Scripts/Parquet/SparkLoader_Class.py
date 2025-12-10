from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import time
import os

AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

class SparkLoader:
    def __init__(self, num_cores="*"):
        self.spark = (
                SparkSession
                .builder
                .master(f"local[{num_cores}]")
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

        # Set log level to reduce verbosity (options: ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF)
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Ignore corrupt inputs at SQL layer to skip bad .gz files
        self.spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
        self.spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

        return None

    def get_chain_store_mapping(self, bucket_name="naya-finalproject-sources", prefix="Chains.csv"):
        s3_source_path = f"s3a://{bucket_name}/{prefix}"
        chains_df = self.spark.read.csv(s3_source_path, header=True, inferSchema=True)
        return chains_df

    def load_xml(self, bucket_name, prefix, row_tag, header_tag, schema=None, header_schema=None, history=False, partitions=None):
        
        if history:
            s3_source_path = f"s3a://{bucket_name}/*/{prefix}" # read historical data
        else:
            today_prefix = time.strftime("%Y-%m-%d")
            s3_source_path = f"s3a://{bucket_name}/{today_prefix}/{prefix}/**/*.gz"

        if partitions is not None:
            df = (
                self.spark
                    .read
                    .format("com.databricks.spark.xml")
                    .option("rowTag", row_tag)
                    .option("recursiveFileLookup", "true")
                    .option("attributePrefix", "_")
                    # .option("pathGlobFilter", "PromoFull*.gz")
                    .option("ignoreNamespace", "true")
                    .schema(schema)
                    .load(s3_source_path)
                    .coalesce(partitions)  # reduce partitions early
            )
        else:
            df = (
                self.spark
                    .read
                    .format("com.databricks.spark.xml")
                    .option("rowTag", row_tag)
                    .option("recursiveFileLookup", "true")
                    .option("attributePrefix", "_")
                    # .option("pathGlobFilter", "PromoFull*.gz")
                    .option("ignoreNamespace", "true")
                    .schema(schema)
                    .load(s3_source_path)
            )
        
        root_meta = (
                self.spark
                .read
                .format("com.databricks.spark.xml")
                .option("rowTag", header_tag)
                .option("recursiveFileLookup", "true")
                .option("ignoreNamespace", "true")
                .schema(header_schema)
                .load(s3_source_path)
                .select(
                    F.input_file_name().cast(T.StringType()).alias("src"),
                    F.col("ChainId").cast(T.StringType()).alias("ChainID"),
                    F.col("StoreId").cast(T.StringType()).alias("StoreID"),
                )
                .dropDuplicates(["ChainID", "StoreID", "src"])
                .withColumn("KeyDate", F.regexp_extract(F.col("src"), r"(\d{4}-\d{2}-\d{2})", 1))
        )

        # root_meta.show(5, truncate=False)
        s3_chains_path = f"s3a://{bucket_name}/chains/Chains.csv"
        chains_df = self.spark.read.csv(s3_chains_path, header=True, inferSchema=True)
        # chains_df.show(10, truncate=False)
        
        # join meta
        df = (
            df
            .withColumn("src", F.input_file_name())
            .join(F.broadcast(root_meta), on="src", how="inner")
            .join(F.broadcast(chains_df), on="ChainID", how="inner")
            .drop("src")
        )

        # store for downstream methods
        self.df = df
        return df

    def load_PromotionDetails(self, nested_count=True, bucket_name="naya-finalproject-processed", prefix="PromotionDetails"):
        
        base_path = f"s3a://{bucket_name}/{prefix}"
        print("\n\n", ">>> Starting processing data for PromotionDetails...")
        try:
            # build details DF
            if nested_count:
                df_PromotionDetails = (
                    self.df
                    .withColumn("NumOfProducts", F.col("PromotionItems._Count").cast(T.IntegerType()))
                )
            else:
                df_PromotionDetails = (
                    self.df
                    .groupBy("ChainID", "ChainNameHeb", "ChainNameEng", "StoreID",
                            "PromotionID", "PromotionDescription",
                            "PromotionStartDate", "PromotionEndDate", "MinQty", "MaxQty",
                            "DiscountRate", "DiscountedPrice", "KeyDate", "ChainPrefix")
                    .count()
                    .withColumnRenamed("count", "NumOfProducts")
                )
            
            # df_PromotionDetails.show(5, truncate=False)
            
            df_PromotionDetails = (
                df_PromotionDetails
                .select(
                    F.col("ChainID").cast(T.StringType()).alias("ChainID"),
                    F.col("ChainNameHeb").cast(T.StringType()).alias("ChainNameHeb"),
                    F.col("ChainNameEng").cast(T.StringType()).alias("ChainNameEng"),
                    F.lpad(F.col("StoreID").cast(T.StringType()), 3, "0").alias("StoreID"),
                    F.col("PromotionId").cast(T.StringType()).alias("PromotionID"),
                    F.col("PromotionDescription").cast(T.StringType()).alias("PromotionDescription"),
                    F.to_date(F.col("PromotionStartDate")).cast(T.DateType()).alias("PromotionStartDate"),
                    F.to_date(F.col("PromotionEndDate")).cast(T.DateType()).alias("PromotionEndDate"),
                    F.col("MinQty").cast(T.IntegerType()).alias("MinQty"),
                    F.col("MaxQty").cast(T.IntegerType()).alias("MaxQty"),
                    F.col("DiscountRate").cast(T.DoubleType()).alias("DiscountRate"),
                    F.col("DiscountedPrice").cast(T.DoubleType()).alias("DiscountedPrice"),
                    F.col("NumOfProducts").cast(T.IntegerType()).alias("NumOfProducts"),
                    F.col("KeyDate").cast(T.StringType()).alias("KeyDate"),
                    F.col("ChainPrefix").cast(T.StringType()).alias("ChainPrefix")
                )
            )
            
            # df_PromotionDetails.show(5, truncate=False)
            # df_PromotionDetails.select(F.col("DiscountRate"), F.col("DiscountedPrice")).show(5, truncate=False)
            # write per (KeyDate, ChainID) to desired prefix segments without partition column names
            
            pairs = (
                df_PromotionDetails
                .select("KeyDate", "ChainPrefix")
                .dropDuplicates()
                .collect()
            )
            for row in pairs:
                key_date = row["KeyDate"]
                chain_prefix = row["ChainPrefix"]
                out_path = f"{base_path}/{key_date}/{chain_prefix}"
                subset = df_PromotionDetails.filter((F.col("KeyDate") == key_date) & (F.col("ChainPrefix") == chain_prefix))
                print(f"Writing PromotionDetails to: {out_path}")
                subset.write.mode("overwrite").parquet(out_path)
            print(f">>>{base_path} written successfully.")
        except Exception as e:
            raise Exception(f"Error in load_PromotionDetails: {e}")
        
        return True

    def load_PromotionItems(self, explode=True, bucket_name="naya-finalproject-processed", prefix="PromotionItems"):
        print("\n\n", ">>> Starting processing data for PromotionItems...")
        try:
            base_path = f"s3a://{bucket_name}/{prefix}"
            if explode:
                df_items = (
                    self.df
                    .select(
                        F.col("PromotionId").alias("PromotionID"),
                        F.explode_outer(F.col("PromotionItems.Item")).alias("Item"),
                        F.col("ChainPrefix").cast(T.StringType()).alias("ChainPrefix"),
                        F.col("KeyDate").cast(T.StringType()).alias("KeyDate")
                    )
                    .select(
                    F.col("PromotionID").cast(T.StringType()).alias("PromotionID"),
                    F.col("Item.ItemCode").cast(T.StringType()).alias("ItemCode"),
                    F.col("ChainPrefix").cast(T.StringType()).alias("ChainPrefix"), 
                    F.col("KeyDate").cast(T.StringType()).alias("KeyDate")
                    )
                    .dropDuplicates(["PromotionID", "ItemCode", "ChainPrefix", "KeyDate"])
                )
            else:
                df_items = (
                    self.df
                    .select(
                        F.col("PromotionId").cast(T.StringType()).alias("PromotionID"),
                        F.col("ItemCode").cast(T.StringType()).alias("ItemCode"),
                        F.col("ChainPrefix").cast(T.StringType()).alias("ChainPrefix"),
                        F.col("KeyDate").cast(T.StringType()).alias("KeyDate")
                    )
                    .dropDuplicates(["PromotionID", "ItemCode", "ChainPrefix", "KeyDate"])
                )

            # write per (KeyDate, ChainID)
            pairs = (
                df_items
                .select("KeyDate", "ChainPrefix")
                .dropDuplicates()
                .collect()
            )
            for row in pairs:
                key_date = row["KeyDate"]
                chain_prefix = row["ChainPrefix"]
                out_path = f"{base_path}/{key_date}/{chain_prefix}"
                subset = df_items.filter((F.col("KeyDate") == key_date) & (F.col("ChainPrefix") == chain_prefix))
                print(f"Writing PromotionItems to: {out_path}")
                subset.write.mode("overwrite").parquet(out_path)
            print(f">>>{base_path} written successfully.")
        
        except Exception as e:
            print(f"Error in load_PromotionItems: {e}")
            return False

        return True

    def stop_spark(self):
        self.spark.stop()
        return True