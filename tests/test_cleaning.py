from src.cleaning import clean_customers
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date

def test_clean_customers_deduplication(spark):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("state", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("_corrupt_record", StringType(), True)
    ])
    
    # C1 is duplicated. The second entry has a later date.
    data = [
        ("C1", "mh", date(2023, 1, 1), None),
        ("C1", "dl", date(2023, 5, 1), None),
        ("C2", "ka", date(2023, 2, 1), None)
    ]
    df = spark.createDataFrame(data, schema)
    
    cleaned = clean_customers(df)
    
    # Assert row count dropped from 3 to 2
    assert cleaned.count() == 2
    
    # Assert the retained C1 row is the newer one with capitalized 'DL'
    c1_row = cleaned.filter(cleaned.customer_id == "C1").first()
    assert c1_row.state == "DL"
    assert c1_row.signup_date == date(2023, 5, 1)
