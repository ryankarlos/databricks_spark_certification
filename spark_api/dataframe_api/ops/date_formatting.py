from spark_session import create_spark_session
from pyspark.sql.functions import *
from spark_api.dataframe_api.io import read_dataset_into_df

spark = create_spark_session("date_formatting")
file = "datasets/sf-fire/sf-fire-calls.csv"
fire_df = read_dataset_into_df(file, format="csv")
fire_df.show(10)

# Convert the existing column’s data type from string to a Spark-supported timestamp.
# Use the new format specified in the format string "MM/dd/yyyy" or "MM/dd/yyyy hh:mm:ss a" where appropriate.
# Drop the old str format date column

fire_ts_df = (
    fire_df.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn(
        "AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop("AvailableDtTm")
)
(fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False))


# we can query using functions from spark.sql.functions like month(), year(), and day() to explore our data further.
# We could find out how many calls were logged in the last seven days, or we could see how many years’ worth of
# Fire Department calls are included in the data set with this query:
(
    fire_ts_df.select(year("IncidentDate"))
    .distinct()
    .orderBy(year("IncidentDate"))
    .show()
)
