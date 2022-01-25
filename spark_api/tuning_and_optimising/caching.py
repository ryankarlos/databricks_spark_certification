from spark_session import create_spark_session
from spark_api.dataframe_api.io import read_dataset_into_df
from timer import Timer

spark = create_spark_session("caching")
spark.conf.set("spark.sql.debug.maxToStringFields", 30)
spark.catalog.clearCache()
firepath = "datasets/sf-fire/sf-fire-calls.csv"
df = read_dataset_into_df(firepath, "csv")

with Timer():
    df.orderBy("CallDate").count()

# A call to cache() does not immediately materialize the data in cache.
# An action using the DataFrame must be executed for Spark to actually cache the data.
df.cache()
df.count()

# Observe change in execution time after caching below.
with Timer():
    df.orderBy("CallDate").count()

# Removes cache for a DataFrame
df.unpersist()
