from pyspark.sql.functions import col, when, array_union, array, lit

def check_order_integrity(orders_df, valid_customers_df, valid_products_df):
    """
    Checks if an order's customer_id and product_id exist in the valid dimensions.
    Appends to the failure_reasons array if missing.
    """
    # 1. Extract just the IDs from the valid dimensions and rename them to avoid column name collisions.
    cust_ids = valid_customers_df.select(col("customer_id").alias("valid_cust_id")).dropDuplicates()
    prod_ids = valid_products_df.select(col("product_id").alias("valid_prod_id")).dropDuplicates()
    
    # 2. Left Join Orders to these ID lists. 
    # If a match exists, valid_cust_id will be populated. If not, it will be Null.
    enriched = orders_df.join(cust_ids, orders_df.customer_id == col("valid_cust_id"), "left")
    enriched = enriched.join(prod_ids, orders_df.product_id == col("valid_prod_id"), "left")
    
    # 3. Append to the failure_reasons array using array_union if the joined ID is Null.
    enriched = enriched.withColumn(
        "failure_reasons",
        when(col("valid_cust_id").isNull(), array_union(col("failure_reasons"), array(lit("Missing Customer Reference"))))
        .otherwise(col("failure_reasons"))
    )
    
    enriched = enriched.withColumn(
        "failure_reasons",
        when(col("valid_prod_id").isNull(), array_union(col("failure_reasons"), array(lit("Missing Product Reference"))))
        .otherwise(col("failure_reasons"))
    )
    
    # 4. Clean up the temporary join columns
    return enriched.drop("valid_cust_id", "valid_prod_id")
