from src.validation import validate_products
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from decimal import Decimal

def test_validate_products_multiple_failures(spark):
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("active_status", StringType(), True)
    ])
    
    # Null ID, negative price, and invalid status 'X' (3 failures)
    data = [(None, Decimal('-10.00'), "X")]
    df = spark.createDataFrame(data, schema)
    
    validated = validate_products(df)
    reasons = validated.first().failure_reasons
    
    assert len(reasons) == 3
    assert "Missing Product ID" in reasons
    assert "Price must be > 0" in reasons
    assert "Invalid Active Status" in reasons
