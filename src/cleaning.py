from pyspark.sql.functions import col, trim, upper, when, row_number
from pyspark.sql.window import Window

def clean_strings(df):
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string" and field.name != "_corrupt_record":
            c = field.name
            df = df.withColumn(c, trim(col(c)))
            df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))
    return df

def clean_customers(df):
    df = clean_strings(df)
    df = df.withColumn("state", upper(col("state")))
    
    window_spec = Window.partitionBy("customer_id").orderBy(col("signup_date").desc_nulls_last())
    df = df.withColumn("row_num", row_number().over(window_spec)) \
           .filter(col("row_num") == 1) \
           .drop("row_num")
    return df

def clean_products(df):
    df = clean_strings(df)
    df = df.withColumn("category", upper(col("category")))
    df = df.withColumn("active_status", upper(col("active_status")))
    return df.dropDuplicates(["product_id"])

def clean_orders(df):
    return clean_strings(df)
