from pyspark.sql.functions import col, count, when, isnan

def profile_dataframe(df, dataset_name):
    print(f"--- Profiling Report: {dataset_name} ---")
    total_rows = df.count()
    print(f"Total Rows: {total_rows}")
    
    null_exprs = [
        count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) 
        for c in df.columns if c != "_corrupt_record"
    ]
    
    print("Null counts per column:")
    df.select(*null_exprs).show()
    print("-" * 40)
