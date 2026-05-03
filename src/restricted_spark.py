from pyspark.sql import SparkSession
import re


def extract_tables(query):
    matches = re.findall(r'from\s+([\w\.]+)', query, re.I)
    return matches

class RestrictedReader:
    def __init__(self, spark, allowed_tables):
        self.spark = spark
        self.allowed_tables = allowed_tables

    def table(self, table_name: str):

        if table_name not in self.allowed_tables:
            raise Exception(
                f"Operation NOT SUPPORTED.\n"
                f"Table '{table_name}' not declared in dependencies."
            )
        return self.spark.read.table(table_name)



class RestrictedSparkSession:

    def __init__(self, spark: SparkSession, allowed_tables: list[str]):
        self._spark = spark
        self._allowed_tables = set(allowed_tables)

    @property
    def read(self):
        return RestrictedReader(self._spark, self._allowed_tables)

    def sql(self, query: str):
        for table in extract_tables(query):
            if table not in self._allowed_tables:
                raise Exception(
                    f"Unauthorized table access: {table}"
                )
        return self._spark.sql(query)

    def __getattr__(self, item):
        # forward everything else
        return getattr(self._spark, item)