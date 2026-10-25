from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType
)

def get_spark_session(app_name="DataQualityEngine"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("_corrupt_record", StringType(), True)
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("active_status", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("discount", DecimalType(3, 2), True),
    StructField("order_status", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])
