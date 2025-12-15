import SparkLoader_Class as SC
import pyspark.sql.types as T

# Schema aligned to Hatzihinam XML
promotion_item_schema = T.StructType([
    T.StructField("ItemCode", T.StringType(), True),
    T.StructField("ItemType", T.IntegerType(), True),
    T.StructField("IsGiftItem", T.IntegerType(), True),
])

additional_restrictions_schema = T.StructType([
    T.StructField("AdditionalIsActive", T.IntegerType(), True),
])

schema = T.StructType([
    T.StructField("PromotionID", T.StringType(), True),
    T.StructField("PromotionDescription", T.StringType(), True),
    T.StructField("PromotionUpdateTime", T.StringType(), True),
    T.StructField("PromotionStartDate", T.StringType(), True),
    T.StructField("PromotionStartHour", T.StringType(), True),
    T.StructField("PromotionEndDate", T.StringType(), True),
    T.StructField("PromotionEndHour", T.StringType(), True),
    T.StructField("AllowMultipleDiscounts", T.IntegerType(), True),
    T.StructField("RewardType", T.IntegerType(), True),
    T.StructField("IsWeightedPromo", T.IntegerType(), True),
    T.StructField("AdditionalRestrictions", additional_restrictions_schema, True),
    T.StructField("MinQty", T.IntegerType(), True),
    T.StructField("MaxQty", T.IntegerType(), True),
    T.StructField("MinPurchaseAmount", T.DoubleType(), True),
    T.StructField("DiscountType", T.IntegerType(), True),
    T.StructField("DiscountedPrice", T.DoubleType(), True),
    T.StructField("DiscountRate", T.DoubleType(), True),
    T.StructField("DiscountedPricePerMida", T.DoubleType(), True),
    T.StructField("PromotionItems", T.StructType([
        T.StructField("Item", T.ArrayType(promotion_item_schema), True)
    ]), True),
])

# # Read root metadata (uppercase tag names)
# header_schema = T.StructType([
#     T.StructField("ChainID", T.StringType(), True),
#     T.StructField("SubChainID", T.StringType(), True),
#     T.StructField("StoreID", T.StringType(), True),
#     T.StructField("BikoretNo", T.IntegerType(), True),
# ])

spark = SC.SparkLoader()
df = spark.load_xml(bucket_name="naya-finalproject-sources",
                    prefix="hatzihinam",
                    row_tag="Promotion",
                    schema=schema
                )
spark.load_PromotionDetails(nested_count=False)
spark.load_PromotionItems(explode=True)
spark.stop_spark()