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

promo_schema = T.StructType([
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

spark = SC.SparkLoader()
df_PromoFull = spark.load_xml(bucket_name="naya-finalproject-sources",
                    chain="ramilevi",
                    df="PromoFull",
                    row_tag="Promotion",
                    schema=promo_schema
                    )

df_items = spark.load_xml(bucket_name="naya-finalproject-sources",
                    chain="ramilevi",
                    df="PriceFull",
                    row_tag="Item",
                    schema=items_schema
                    )
spark.load_PromotionDetails(nested_count=False)
spark.load_PromotionItems(explode=True)
spark.stop_spark()