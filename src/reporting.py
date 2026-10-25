def write_parquet(df, output_path):
    """
    Writes a DataFrame to disk in Parquet format.
    Using 'overwrite' ensures idempotency (repeated runs don't duplicate data).
    """
    df.write.mode("overwrite").parquet(output_path)

def generate_reconciliation_report(dataset_name, raw_count, cleaned_count, valid_count, invalid_count):
    """
    Calculates the pipeline math: Raw = Valid + Invalid + Dropped Duplicates.
    """
    dropped_duplicates = raw_count - cleaned_count
    total_accounted = valid_count + invalid_count + dropped_duplicates
    match = raw_count == total_accounted
    
    print(f"\n--- Reconciliation Report: {dataset_name} ---")
    print(f"Raw Input Records:     {raw_count}")
    print(f"Dropped Duplicates:    {dropped_duplicates}")
    print(f"Valid Records:         {valid_count}")
    print(f"Quarantined Records:   {invalid_count}")
    
    if match:
        print(f"Status: SUCCESS. All {raw_count} records accounted for.")
    else:
        print(f"Status: FAILED. Missing {raw_count - total_accounted} records.")
