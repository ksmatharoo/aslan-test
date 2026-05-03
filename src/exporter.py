from pyspark.sql import SparkSession
from registry import JobRegistry


def export_jobs_to_delta(spark, table_name):

    jobs = JobRegistry.get_jobs()

    job_records = []
    dependency_records = []

    for job in jobs.values():

        job_records.append({
            "jobName": job.jobName,
            "alias": job.alias,
            "dagName": job.dagName
        })

        for dep in job.dependencyList:
            dependency_records.append({
                "jobName": job.jobName,
                "dependsOn": dep
            })

    spark.createDataFrame(job_records) \
        .write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{table_name}_jobs")

    spark.createDataFrame(dependency_records) \
        .write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{table_name}_dependencies")