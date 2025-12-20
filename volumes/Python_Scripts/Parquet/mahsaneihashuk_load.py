import Spark_Load_Parquet as SC
import pyspark.sql.types as T

promo_schema = T.StructType([
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

items_schema = T.StructType([
    T.StructField("PriceUpdateDate", T.StringType(), True),
    T.StructField("ItemCode", T.StringType(), True),
    T.StructField("ItemType", T.IntegerType(), True),
    T.StructField("ItemName", T.StringType(), True),
    T.StructField("ManufactureName", T.StringType(), True),
    T.StructField("ManufactureCountry", T.StringType(), True),
    T.StructField("ManufactureItemDescription", T.StringType(), True),
    T.StructField("UnitQty", T.StringType(), True),
    T.StructField("Quantity", T.DoubleType(), True),
    T.StructField("UnitMeasure", T.StringType(), True),
    T.StructField("BisWeighted", T.IntegerType(), True),
    T.StructField("QtyInPackage", T.StringType(), True),
    T.StructField("ItemPrice", T.DoubleType(), True),
    T.StructField("UnitOfMeasurePrice", T.DoubleType(), True),
    T.StructField("AllowDiscount", T.IntegerType(), True),
    T.StructField("itemStatus", T.IntegerType(), True),
    T.StructField("LastUpdateDate", T.StringType(), True),
    T.StructField("LastUpdateTime", T.StringType(), True),
])


spark = SC.SparkLoader()
df_PromoFull = spark.load_xml(bucket_name="naya-finalproject-sources",
                    chain="mahsaneihashuk",
                    df="PromoFull",
                    row_tag="Sale",
                    schema=promo_schema
                    )

df_items = spark.load_xml(bucket_name="naya-finalproject-sources",
                    chain="mahsaneihashuk",
                    df="PriceFull",
                    row_tag="Product",
                    schema=items_schema
                    )

spark.load_PromotionDetails(nested_count=False)
spark.load_PromotionItems(explode=False)
spark.stop_spark()