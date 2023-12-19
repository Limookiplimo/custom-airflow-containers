from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging


def run_spark_script():
    try:
        spark = SparkSession.builder \
        .appName("SimplePySparkApp") \
        .config("spark.master", "spark://localhost:7077") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])

        data = [(1, "John", 25),
                (2, "Jane", 30),
                (3, "Bob", 22)]

        df = spark.createDataFrame(data, schema=schema)

        df.show()
        spark.stop()
    except Exception as e:
        logging.error(f"Error in Spark job: {e}")
        raise


run_spark_script()