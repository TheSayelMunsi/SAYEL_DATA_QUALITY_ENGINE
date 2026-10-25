from pyspark.sql.functions import col, sum as _sum, count, rank, desc, round as _round, lit
from pyspark.sql.window import Window

def create_enriched_orders(valid_orders, valid_customers, valid_products):
    """Joins valid orders with customer and product details, and calculates net revenue."""
    # Apply aliases to the DataFrames to avoid column ambiguity
    o = valid_orders.alias("o")
    c = valid_customers.alias("c")
    p = valid_products.alias("p")
    
    enriched = o.join(c, "customer_id", "inner") \
                .join(p, "product_id", "inner")
    
    # Explicitly reference the unit_price and discount from the Orders (o) table
    enriched = enriched.withColumn(
        "net_revenue", 
        _round(col("o.quantity") * col("o.unit_price") * (lit(1.0) - col("o.discount")), 2)
    )
    return enriched

def get_completed_sales(enriched_orders):
    return enriched_orders.filter(col("order_status") != "CANCELLED")

def analyze_revenue_by_product(completed_sales):
    return completed_sales.groupBy("product_id", "product_name") \
                          .agg(_sum("net_revenue").alias("total_revenue")) \
                          .orderBy(desc("total_revenue"))

def analyze_revenue_by_category(completed_sales):
    return completed_sales.groupBy("category") \
                          .agg(_sum("net_revenue").alias("total_revenue")) \
                          .orderBy(desc("total_revenue"))

def analyze_daily_sales(completed_sales):
    return completed_sales.groupBy("order_date") \
                          .agg(_sum("net_revenue").alias("daily_revenue"),
                               count("order_id").alias("total_orders")) \
                          .orderBy("order_date")

def analyze_top_products_by_category(completed_sales):
    category_revenue = completed_sales.groupBy("category", "product_id", "product_name") \
                                      .agg(_sum("net_revenue").alias("total_revenue"))
                                      
    window_spec = Window.partitionBy("category").orderBy(desc("total_revenue"))
    
    ranked = category_revenue.withColumn("revenue_rank", rank().over(window_spec))
    return ranked.filter(col("revenue_rank") <= 3)

def analyze_customer_spending(completed_sales):
    return completed_sales.groupBy("customer_id", "first_name", "last_name") \
                          .agg(_sum("net_revenue").alias("total_spent"),
                               count("order_id").alias("total_orders")) \
                          .orderBy(desc("total_spent"))
