from spark_session import create_spark_session
from pyspark.sql.functions import *
from spark_api.dataframe_api.io import read_dataset_into_df

# Create a SparkSession
spark = create_spark_session("filter")
fire_df = read_dataset_into_df()


few_fire_df = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType")\
    .filter((col("CallType") != "Medical Incident") & col("AvailableDtTm").isNotNull())\
    .sort(col("AvailableDtTm")).limit(50)

few_fire_df.show(truncate=False)


# Return number of distinct types of calls using countDistinct()
fire_df.select("CallType").where(col("CallType").isNotNull()).agg(
    countDistinct("CallType").alias("DistinctCallTypes")
).show()


# filter for only distinct non-null CallTypes from all the rows
fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().show(10, False)
