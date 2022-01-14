from spark_session import create_spark_session
from spark_api.dataframe_api.io import read_dataset_into_df
from pyspark.sql.functions import col

spark = create_spark_session("optimiser")

firepath = "datasets/sf-fire/sf-fire-calls.csv"
df = read_dataset_into_df(firepath, "csv")

firecallsDF = (
    df.filter(col("CallType") != "Structure Fire")
    .filter(col("CallType") != "Medical Incident")
    .filter(col("CallType") != "Vehicle Fire")
)

firecallsDF.count()

# Prints the plans (logical and physical), optionally formatted by a given explain mode
firecallsDF.explain(True)

betterDF = df.filter(
    (col("CallType").isNotNull())
    & (col("CallType") != "Structure Fire")
    & (col("CallType") != "Medical Incident")
    & (col("CallType") != "Vehicle Fire")
)

betterDF.count()

betterDF.explain(True)


stupidDF = (
    df.filter(col("CallType") != "Structure Fire")
    .filter(col("CallType") != "Structure Fire")
    .filter(col("CallType") != "Structure Fire")
)


stupidDF.explain(True)
