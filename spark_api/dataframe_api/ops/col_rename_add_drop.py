from spark_session import create_spark_session
from pyspark.sql.functions import *
from spark_api.dataframe_api.io import read_csv_into_df

# Create a SparkSession
spark = create_spark_session("filter")
fire_df = read_csv_into_df()

new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(
    new_fire_df.select("ResponseDelayedinMins")
    .where(col("ResponseDelayedinMins") > 5)
    .show(5, False)
)


fire_ts_df = (
    new_fire_df.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn(
        "AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop("AvailableDtTm")
)
# Select the converted columns
(fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False))


(
    fire_ts_df.select(year("IncidentDate"))
    .distinct()
    .orderBy(year("IncidentDate"))
    .show()
)
