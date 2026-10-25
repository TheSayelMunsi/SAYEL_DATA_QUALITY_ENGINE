from pyspark.sql.functions import col, when, lit, array, size, filter

def validate_customers(df):
    r_id = when(col("customer_id").isNull() | (col("customer_id") == ""), lit("Missing Customer ID")).otherwise(lit(None))
    r_email = when(~col("email").contains("@"), lit("Invalid Email format")).otherwise(lit(None))
    
    df = df.withColumn("all_reasons", array(r_id, r_email))
    # Use PySpark's higher-order filter function to strip nulls from the array
    df = df.withColumn("failure_reasons", filter(col("all_reasons"), lambda x: x.isNotNull()))
    return df.drop("all_reasons")

def validate_products(df):
    r_id = when(col("product_id").isNull(), lit("Missing Product ID")).otherwise(lit(None))
    r_price = when(col("unit_price") <= 0, lit("Price must be > 0")).otherwise(lit(None))
    r_status = when(~col("active_status").isin("Y", "N"), lit("Invalid Active Status")).otherwise(lit(None))
    
    df = df.withColumn("all_reasons", array(r_id, r_price, r_status))
    df = df.withColumn("failure_reasons", filter(col("all_reasons"), lambda x: x.isNotNull()))
    return df.drop("all_reasons")

def validate_orders(df):
    r_id = when(col("order_id").isNull(), lit("Missing Order ID")).otherwise(lit(None))
    r_qty = when(col("quantity") <= 0, lit("Quantity must be > 0")).otherwise(lit(None))
    r_price = when(col("unit_price") < 0, lit("Price cannot be negative")).otherwise(lit(None))
    r_discount = when((col("discount") < 0.0) | (col("discount") > 1.0), lit("Invalid Discount")).otherwise(lit(None))
    r_status = when(~col("order_status").isin("PLACED", "SHIPPED", "DELIVERED", "CANCELLED"), lit("Invalid Order Status")).otherwise(lit(None))
    
    df = df.withColumn("all_reasons", array(r_id, r_qty, r_price, r_discount, r_status))
    df = df.withColumn("failure_reasons", filter(col("all_reasons"), lambda x: x.isNotNull()))
    return df.drop("all_reasons")

def split_valid_invalid(df):
    """
    Separates a validated DataFrame into clean records and quarantined records.
    Also handles structural CSV parsing failures (_corrupt_record).
    """
    is_corrupt = col("_corrupt_record").isNotNull()
    has_failures = size(col("failure_reasons")) > 0
    
    # If the CSV parser failed on this row, forcefully add that to the reasons.
    df = df.withColumn("failure_reasons", 
                       when(is_corrupt, array(lit("Malformed CSV Row")))
                       .otherwise(col("failure_reasons")))
    
    # Valid rows have no parsing errors and no business rule failures.
    valid_df = df.filter(~is_corrupt & ~has_failures).drop("failure_reasons", "_corrupt_record")
    
    # Invalid rows keep their original data plus the failure reasons.
    invalid_df = df.filter(is_corrupt | has_failures).drop("_corrupt_record")
    
    return valid_df, invalid_df
