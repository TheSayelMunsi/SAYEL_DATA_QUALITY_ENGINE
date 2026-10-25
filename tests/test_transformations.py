from src.transformations import create_enriched_orders
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from decimal import Decimal

def test_net_revenue_calculation(spark):
    # Quantity: 2, Price: 100.00, Discount: 10% (0.10)
    # Expected Revenue = 2 * 100 * 0.9 = 180.00
    o_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10,2), True),
        StructField("discount", DecimalType(3,2), True)
    ])
    o_data = [("O1", "C1", "P1", 2, Decimal('100.00'), Decimal('0.10'))]
    df_o = spark.createDataFrame(o_data, o_schema)

    # Mock the dimension DataFrames required for the join
    c_schema = StructType([StructField("customer_id", StringType(), True)])
    df_c = spark.createDataFrame([("C1",)], c_schema)

    p_schema = StructType([StructField("product_id", StringType(), True)])
    df_p = spark.createDataFrame([("P1",)], p_schema)

    enriched = create_enriched_orders(df_o, df_c, df_p)
    revenue = enriched.first().net_revenue

    assert revenue == Decimal('180.00')
