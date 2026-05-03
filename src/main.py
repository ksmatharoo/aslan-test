from pyspark.sql import SparkSession
from decorators import job
from registry import JobRegistry


# @job(
#     jobName="job_A",
#     dependencyList=[],
#     alias="A",
#     dagName="example_dag"
# )
# def job_a():
#     print("Job A running")
#
#
# @job(
#     jobName="job_B",
#     dependencyList=["job_A"],
#     alias="B",
#     dagName="example_dag"
# )
# def job_b():
#     print("Job B running")


@job(
    jobName="customer_gold",
    dependencyList=[
        "silver.customer",
        "silver.address"
    ],
    alias="cust_gold",
    dagName="customer_dag"
)
def build_customer(spark_session):
    # df1 = spark.sql("select * from silver.customer1")
    # df1.count()

    df1 = spark_session.read.table("silver.customer")
    addr = spark_session.read.table("silver.address")

    return df1.join(addr, "customer_id")

if __name__ == '__main__':
    from pathlib import Path

    script_path = Path(__file__).resolve().parent
    warehouse_path = script_path.parent / "spark-warehouse"

    print(warehouse_path)

    spark = (SparkSession.builder
             .appName("DeltaLocalWrite")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .master("local[2]")
             .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.warehouse.dir", warehouse_path)
             .enableHiveSupport()
             .getOrCreate())

    df = build_customer(spark)

    df.show(10)

    # call method without decorator
    #build_customer.__wrapped__(spark)

    print(df.count())