from pyspark.sql.session import SparkSession


def get_session() -> SparkSession:
    return SparkSession.builder \
        .config("spark.executor.memory", "16g") \
        .config("spark.driver.memory", "16g") \
        .getOrCreate()

