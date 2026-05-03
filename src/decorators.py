from functools import wraps
from pyspark.sql import DataFrame
from registry import JobRegistry
from models import JobDefinition
from restricted_spark import RestrictedSparkSession


def job(jobName, dependencyList, alias, dagName):

    def decorator(func):

        job_def = JobDefinition(
            jobName,
            dependencyList,
            alias,
            dagName,
            func
        )

        JobRegistry.register(job_def)

        @wraps(func)
        def wrapper(spark, *args, **kwargs):

            restricted_spark = RestrictedSparkSession(
                spark,
                dependencyList
            )

            result = func(restricted_spark, *args, **kwargs)

            # enforce return type
            if not isinstance(result, DataFrame):
                raise TypeError(
                    f"{jobName} must return a PySpark DataFrame"
                )

            return result

        return wrapper

    return decorator