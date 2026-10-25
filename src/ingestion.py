def read_csv_with_schema(spark, file_path, schema):
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(schema) \
        .load(file_path)
