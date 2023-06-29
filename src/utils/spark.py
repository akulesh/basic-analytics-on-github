import os

from pyspark.sql import SparkSession


os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


def get_spark_session(app_name="github", spark_memory="4g"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.executor.memory", spark_memory)
        .config("spark.driver.memory", spark_memory)
        .config("spark.sql.debug.maxToStringFields", 1000)
    ).getOrCreate()
