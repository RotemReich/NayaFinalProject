import SparkLoader_Class as SC
import pyspark.sql.types as T

# Schema aligned to provided XML
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
    T.StructField("PromotionDescription", T.StringType(), True),
    T.StructField("PromotionUpdateDate", T.StringType(), True),
    T.StructField("PromotionStartDate", T.StringType(), True),
    T.StructField("PromotionStartHour", T.StringType(), True),
    T.StructField("PromotionEndDate", T.StringType(), True),
    T.StructField("PromotionEndHour", T.StringType(), True),
    T.StructField("RewardType", T.IntegerType(), True),
    T.StructField("AllowMultipleDiscounts", T.IntegerType(), True),
    T.StructField("IsWeightedPromo", T.IntegerType(), True),
    T.StructField("AdditionalRestrictions", additional_restrictions_schema, True),
    T.StructField("MinQty", T.DoubleType(), True),
    T.StructField("MaxQty", T.DoubleType(), True),
    T.StructField("DiscountedPrice", T.DoubleType(), True),
    T.StructField("DiscountRate", T.DoubleType(), True),
    T.StructField("DiscountedPricePerMida", T.DoubleType(), True),
    T.StructField("MinNoOfItemOfered", T.IntegerType(), True),
    T.StructField("WeightUnit", T.IntegerType(), True),
    T.StructField("PromotionItems", T.StructType([
        T.StructField("Item", T.ArrayType(promotion_item_schema), True)
    ]), True),
    T.StructField("Clubs", clubs_schema, True),
])

# header schema
header_schema = T.StructType([
    T.StructField("XmlDocVersion", T.StringType(), True),
    T.StructField("ChainId", T.StringType(), True),
    T.StructField("SubChainId", T.StringType(), True),
    T.StructField("StoreId", T.StringType(), True),
    T.StructField("BikoretNo", T.IntegerType(), True),
    T.StructField("DllVerNo", T.StringType(), True),
    T.StructField("Promotions", T.StructType([
        T.StructField("_Count", T.IntegerType(), True)
    ]), True)
])

spark = SC.SparkLoader()
df = spark.load_xml(bucket_name="naya-finalproject-sources",
                    prefix="ramilevi-promofull-gz",
                    row_tag="Promotion",
                    header_tag="Root",
                    schema=schema,
                    header_schema=header_schema)
spark.load_PromotionDetails(nested_count=False)
spark.load_PromotionItems(explode=True)
spark.stop_spark()