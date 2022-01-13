from spark_session import create_spark_session
import pyspark.sql.functions as F
from spark_api.dataframe_api.io import read_dataset_into_df

# Create a SparkSession
spark = create_spark_session("filter")
fire_df = read_dataset_into_df()


(
    fire_df.select("CallType")
    .where(F.col("CallType").isNotNull())
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show(n=10, truncate=False)
)


(
    fire_df.select(
        F.sum("NumAlarms"),
        F.avg("Delay"),
        F.min("Delay"),
        F.max("Delay"),
    ).show()
)
