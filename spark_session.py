import findspark

findspark.init()
findspark.find()

from pyspark.sql import SparkSession

# Create a SparkSession
def create_spark_session(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark
