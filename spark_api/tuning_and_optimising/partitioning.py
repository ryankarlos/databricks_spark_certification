from spark_session import create_spark_session
from spark_api.dataframe_api.io import read_dataset_into_df

spark = create_spark_session("partitioning")

df = read_dataset_into_df('datasets/loans/loan-risks.snappy.parquet', 'parquet')
print(df.rdd.getNumPartitions())

# Access SparkContext through SparkSession to get the number of cores or slots
print(spark.sparkContext.defaultParallelism)

# Returns a new DataFrame that has exactly n partitions.
repartitionedDF = df.repartition(8)
print(repartitionedDF.rdd.getNumPartitions())

# Returns a new DataFrame that has exactly n partitions, when the fewer partitions are requested
# If a larger number of partitions is requested, it will stay at the current number of partitions
coalesceDF = repartitionedDF.coalesce(6)
print(coalesceDF.rdd.getNumPartitions())


# Use SparkConf to access the spark configuration parameter for default shuffle partitions
print(spark.conf.get("spark.sql.shuffle.partitions"))
# configure default shuffle partitions to match number of cores - 32 in my case
spark.conf.set("spark.sql.shuffle.partitions", "32")


# adaptive query execution - dynamically coalesce shuffle partitions at runtime
# control whether AQE is turned on/off (disabled by default)
print(spark.conf.get("spark.sql.adaptive.enabled"))