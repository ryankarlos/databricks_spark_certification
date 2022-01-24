import findspark

findspark.init()
findspark.find()

from pyspark.sql import SparkSession

# Create a SparkSession
def create_spark_session(app_name):
    # increase spark driver memory (default 1gb) to avoid heap space warnings
    spark = (
        SparkSession.builder.appName(app_name)
            .config("spark.sql.execution.arrow.pyspark.enabled", True)
            .config("spark.driver.memory", "16G")
            .config("spark.ui.showConsoleProgress", True)
            .config("spark.sql.repl.eagerEval.enabled", True)
            .config("spark.dynamicAllocation.enabled", True)
            .getOrCreate()
    )
    return spark
