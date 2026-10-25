import os
import sys
from src.config import get_spark_session, CUSTOMER_SCHEMA, PRODUCT_SCHEMA, ORDER_SCHEMA
from src.ingestion import read_csv_with_schema
from src.profiling import profile_dataframe
from src.cleaning import clean_customers, clean_products, clean_orders
from src.validation import validate_customers, validate_products, validate_orders, split_valid_invalid
from src.integrity import check_order_integrity
from src.transformations import (
    create_enriched_orders, get_completed_sales, analyze_revenue_by_category, 
    analyze_top_products_by_category
)
from src.reporting import write_parquet, generate_reconciliation_report

def main():
    spark = get_spark_session()
    
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_in = os.path.join(base_dir, "data", "raw")
    data_out = os.path.join(base_dir, "data", "output")
    
    print("\nStarting Data Quality & Transformation Engine...")
    
    # 1. Ingestion
    raw_cust = read_csv_with_schema(spark, f"{data_in}/customers.csv", CUSTOMER_SCHEMA)
    raw_prod = read_csv_with_schema(spark, f"{data_in}/products.csv", PRODUCT_SCHEMA)
    raw_ord = read_csv_with_schema(spark, f"{data_in}/orders.csv", ORDER_SCHEMA)
    
    # Cache raw inputs as we run multiple actions (count) on them
    raw_cust.cache()
    raw_prod.cache()
    raw_ord.cache()
    
    # 2. Cleaning (Deduplication)
    clean_cust = clean_customers(raw_cust)
    clean_prod = clean_products(raw_prod)
    clean_ord = clean_orders(raw_ord)
    
    # Cache cleaned data for counting
    clean_cust.cache()
    clean_prod.cache()
    clean_ord.cache()

    # 3. Validation
    valid_cust, invalid_cust = split_valid_invalid(validate_customers(clean_cust))
    valid_prod, invalid_prod = split_valid_invalid(validate_products(clean_prod))
    
    # Cache dimensions to prevent re-computation during the integrity check
    valid_cust.cache()
    valid_prod.cache()

    # 4. Integrity Check (Orders depend on Valid Customers and Products)
    val_ord_base = validate_orders(clean_ord)
    val_ord_integrated = check_order_integrity(val_ord_base, valid_cust, valid_prod)
    valid_ord, invalid_ord = split_valid_invalid(val_ord_integrated)
    valid_ord.cache()
    
    # 5. Output Generation (Quarantine)
    write_parquet(invalid_cust, f"{data_out}/quarantine/customers")
    write_parquet(invalid_prod, f"{data_out}/quarantine/products")
    write_parquet(invalid_ord, f"{data_out}/quarantine/orders")
    
    # 6. Output Generation (Clean Valid Data)
    write_parquet(valid_cust, f"{data_out}/clean/customers")
    write_parquet(valid_prod, f"{data_out}/clean/products")
    write_parquet(valid_ord, f"{data_out}/clean/orders")

    # 7. Analytical Transformations
    enriched_orders = create_enriched_orders(valid_ord, valid_cust, valid_prod)
    completed_sales = get_completed_sales(enriched_orders)
    completed_sales.cache()
    
    top_products = analyze_top_products_by_category(completed_sales)
    write_parquet(top_products, f"{data_out}/analytics/top_products_by_category")
    
    # 8. Reconciliation Reporting
    generate_reconciliation_report("Customers", raw_cust.count(), clean_cust.count(), valid_cust.count(), invalid_cust.count())
    generate_reconciliation_report("Products", raw_prod.count(), clean_prod.count(), valid_prod.count(), invalid_prod.count())
    generate_reconciliation_report("Orders", raw_ord.count(), clean_ord.count(), valid_ord.count(), invalid_ord.count())
    
    print("\nPipeline completed successfully. Check data/output/ for Parquet files.")
    spark.stop()

if __name__ == "__main__":
    # Ensure a non-zero exit code if the pipeline crashes
    try:
        main()
    except Exception as e:
        print(f"Pipeline Failed: {e}", file=sys.stderr)
        sys.exit(1)
