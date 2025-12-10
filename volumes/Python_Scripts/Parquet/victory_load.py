import SparkLoader_Class as SC
import pyspark.sql.types as T

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
    T.StructField("ItemCode", T.StringType(), True),
    T.StructField("ItemType", T.StringType(), True),
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

header_schema = T.StructType([
    T.StructField("ChainID", T.StringType(), True),
    T.StructField("SubChainID", T.StringType(), True),
    T.StructField("StoreID", T.StringType(), True),
    T.StructField("BikoretNo", T.StringType(), True)
])

spark = SC.SparkLoader()
df = spark.load_xml(bucket_name="naya-finalproject-sources",
                    prefix="victory-promofull-gz",
                    row_tag="Sale",
                    header_tag="Promos",
                    schema=schema,
                    header_schema=header_schema)
spark.load_PromotionDetails(nested_count=False)
spark.load_PromotionItems(explode=False)
spark.stop_spark()