from pyspark.sql import SparkSession



if __name__ == '__main__':

    # 1. Initialize Spark Session for Spark 4.1.1
    # The 'configure_spark_with_delta_pip' helper ensures the Delta jar is loaded

    spark = SparkSession.builder \
        .appName("DeltaLocalWrite") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .master("local[2]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

    #spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # 2. Create sample data
    data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # 3. Define the local path
    # Note: Use a full path or a relative path like '/tmp/delta_table'
    local_path = "./my_local_delta_table"

    # 4. Write the DataFrame as a Delta table
    df.write.format("delta") \
        .mode("overwrite") \
        .save(local_path)

    print(f"Delta table written successfully to: {local_path}")

    # 5. Verify by reading it back
    df_read = spark.read.format("delta").load(local_path)
    df_read.show()
