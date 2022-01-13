from spark_session import create_spark_session
from pyspark.sql.types import *

spark = create_spark_session("io")

fire_schema = StructType(
    [
        StructField("CallNumber", IntegerType(), True),
        StructField("UnitID", StringType(), True),
        StructField("IncidentNumber", IntegerType(), True),
        StructField("CallType", StringType(), True),
        StructField("CallDate", StringType(), True),
        StructField("WatchDate", StringType(), True),
        StructField("CallFinalDisposition", StringType(), True),
        StructField("AvailableDtTm", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("City", StringType(), True),
        StructField("ZipcodeofIncident", IntegerType(), True),
        StructField("Battalion", StringType(), True),
        StructField("StationArea", StringType(), True),
        StructField("Box", StringType(), True),
        StructField("OrigPriority", StringType(), True),
        StructField("Priority", StringType(), True),
        StructField("FinalPriority", IntegerType(), True),
        StructField("ALSUnit", BooleanType(), True),
        StructField("CallTypeGroup", StringType(), True),
        StructField("NumAlarms", IntegerType(), True),
        StructField("UnitType", StringType(), True),
        StructField("UnitSequenceInCallDispatch", IntegerType(), True),
        StructField("FirePreventionDistrict", StringType(), True),
        StructField("SupervisorDistrict", StringType(), True),
        StructField("Neighborhood", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("RowID", StringType(), True),
        StructField("Delay", FloatType(), True),
    ]
)


# Use the DataFrameReader interface to read a CSV file
def read_csv_into_df(path, schema, format="csv", infer_schema=False):
    fire_df = (
        spark.read.format(format)
        .option("header", True)
        .schema(schema)
        .option("inferSchema", infer_schema)
        .option("mode", "FAILFAST")
        .load(path)
    )
    return fire_df


def write_dataset(df, mode, path=None, table_name=None):
    if mode == "file":
        if path is None:
            raise ValueError("path must be specified for saving as parquet file")
        df.write.option("compression", "snappy").mode("overwrite").parquet(path)
    elif mode == "table":
        if table_name is None:
            raise ValueError("table name must be specified for saving as sql table")
        df.write.mode("overwrite").saveAsTable(table_name)


# Main program
if __name__ == "__main__":
    path = "datasets/sf-fire/sf-fire-calls.csv"
    fire_df = read_csv_into_df(path, fire_schema)
    fire_df.show(10)

    # create temp table in memory
    fire_df.createOrReplaceTempView("Data")
    spark.sql("SELECT * FROM data WHERE CallType LIKE 'Medical%'").show(10)

    # use DataFrameWriter interface to save dataframe as parquet file
    write_dataset(fire_df, mode="file", path="datasets/sf-fire/fire-calls.parquet")
    # or sql table
    write_dataset(fire_df, mode="table", table_name="fire_calls")
