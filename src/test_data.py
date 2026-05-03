from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("DeltaLocalWrite")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .master("local[2]")
             .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.warehouse.dir", "/Users/kulwinder/github/aslan-test/spark-warehouse")
             .enableHiveSupport()
             .getOrCreate())

    customer_data = [
        (1, "John"),
        (2, "Alice")
    ]

    customer_df = spark.createDataFrame(
        customer_data,
        ["customer_id", "customer_name"]
    )

    customer_df.show()

    address_data = [
        (101, 1, "Singapore"),
        (102, 2, "Bombay")
    ]

    address_df = spark.createDataFrame(
        address_data,
        ["address_id", "customer_id", "city"]
    )

    address_df.show()

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    customer_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver.customer")

    address_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver.address")



    spark.sql("SHOW TABLES IN silver").show()
